package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/dev64-kartik/kvstore/internal/vfs"
)

// Snapshot file layout. All integers are little-endian.
//
//	offset 0   magic    u32  0x4E53564B, which is the bytes "KVSN" on disk
//	offset 4   version  u16  1
//	offset 6   reserved u16  0
//	offset 8   seq      u64  last WAL seq the snapshot covers
//	offset 16  count    u64  number of entries
//	offset 24  entries       keyLen uvarint | key | valLen uvarint | value, in ascending key order
//	end-4      crc      u32  CRC32C over every byte before it
//
// Entries are in key order, so the same state always produces the same bytes.
const (
	snapshotMagic       uint32 = 0x4E53564B
	snapshotVersion     uint16 = 1
	snapshotHeaderSize         = 24
	snapshotTrailerSize        = 4
)

// ErrCorrupt means a snapshot file is damaged. Snapshots are installed with an
// atomic rename, so a crash can't produce a partial one: damage means bit rot
// or a bug, never a torn write.
var ErrCorrupt = errors.New("store: corrupt snapshot")

// castagnoli selects CRC32C. It is set once at package load and only read.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ctxCheckEvery is how many entries WriteSnapshot writes between checks for
// cancellation. Checking every entry would cost more than the work itself.
const ctxCheckEvery = 1024

// snapshotName is zero-padded to 20 digits so names sort by seq.
func snapshotName(seq uint64) string {
	return fmt.Sprintf("snap-%020d.snap", seq)
}

// parseSnapshotName returns the seq in a snapshot file name. It accepts only
// names exactly as snapshotName produces them.
func parseSnapshotName(name string) (uint64, bool) {
	digits, ok := strings.CutPrefix(name, "snap-")
	if !ok {
		return 0, false
	}
	digits, ok = strings.CutSuffix(digits, ".snap")
	if !ok || len(digits) != 20 {
		return 0, false
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	return seq, err == nil
}

func snapshotHeader(seq, count uint64) []byte {
	h := make([]byte, snapshotHeaderSize)
	binary.LittleEndian.PutUint32(h[0:4], snapshotMagic)
	binary.LittleEndian.PutUint16(h[4:6], snapshotVersion)
	binary.LittleEndian.PutUint64(h[8:16], seq)
	binary.LittleEndian.PutUint64(h[16:24], count)
	return h
}

// WriteSnapshot durably installs a snapshot of m, covering WAL seqs through
// seq, as dir/snap-<seq>.snap. m is only read, without any lock, so the caller
// passes a Clone taken under its lock and keeps writing to the original.
//
// The caller must not delete the WAL segments this snapshot replaces until
// WriteSnapshot returns nil. Until then, a crash may bring back the previous
// state, and that state needs those segments.
//
// Unlike a WAL append, a snapshot can be abandoned safely at any point before
// the rename, because nothing has been published yet. So ctx is checked while
// writing. On any error, the temp file is removed, so a later attempt can
// create it again.
func WriteSnapshot(ctx context.Context, fsys vfs.FS, dir string, seq uint64, m *Map) (err error) {
	name := snapshotName(seq)
	final := filepath.Join(dir, name)
	tmp := final + ".tmp"

	// As with WAL segments: rename would silently replace an existing snapshot.
	names, err := fsys.List(dir)
	if err != nil {
		return fmt.Errorf("store: write snapshot: %w", err)
	}
	if slices.Contains(names, name) {
		return &fs.PathError{Op: "write snapshot", Path: final, Err: fs.ErrExist}
	}

	f, err := fsys.Create(tmp)
	if err != nil {
		return fmt.Errorf("store: write snapshot: %w", err)
	}
	closed, renamed := false, false
	defer func() {
		if err == nil {
			return
		}
		if !closed {
			err = errors.Join(err, f.Close())
		}
		if !renamed {
			err = errors.Join(err, fsys.Remove(tmp))
		}
	}()

	sw := &snapshotWriter{w: bufio.NewWriterSize(f, 64<<10), crc: crc32.New(castagnoli)}
	sw.write(snapshotHeader(seq, uint64(m.Len())))
	n := 0
	m.Ascend(func(key, value []byte) bool {
		if n%ctxCheckEvery == 0 && sw.err == nil {
			sw.err = ctx.Err()
		}
		n++
		sw.writeBytes(key)
		sw.writeBytes(value)
		return sw.err == nil
	})
	if sw.err != nil {
		return fmt.Errorf("store: write snapshot %s: %w", tmp, sw.err)
	}
	// The trailer goes to the file only: it is the checksum, so it can't be
	// part of what it covers.
	if _, err := sw.w.Write(binary.LittleEndian.AppendUint32(nil, sw.crc.Sum32())); err != nil {
		return fmt.Errorf("store: write snapshot %s: %w", tmp, err)
	}
	if err := sw.w.Flush(); err != nil {
		return fmt.Errorf("store: write snapshot %s: %w", tmp, err)
	}
	// Data durable before the name points at it: a snapshot name visible after
	// a crash always refers to a complete, valid file.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("store: sync snapshot %s: %w", tmp, err)
	}
	closed = true
	if err := f.Close(); err != nil {
		return fmt.Errorf("store: close snapshot %s: %w", tmp, err)
	}
	if err := ctx.Err(); err != nil {
		return err // last chance to abandon; after the rename the snapshot is published
	}
	if err := fsys.Rename(tmp, final); err != nil {
		return fmt.Errorf("store: install snapshot: %w", err)
	}
	renamed = true
	if err := fsys.SyncDir(dir); err != nil {
		return fmt.Errorf("store: sync dir after installing %s: %w", final, err)
	}
	return nil
}

// snapshotWriter feeds every byte to both the file and the running checksum,
// and keeps the first error. Later writes become no-ops, so the entry loop
// needs a single error check at the end ("errors are values").
type snapshotWriter struct {
	w       *bufio.Writer
	crc     hash.Hash32
	err     error
	scratch [binary.MaxVarintLen64]byte
}

func (sw *snapshotWriter) write(p []byte) {
	if sw.err != nil {
		return
	}
	sw.crc.Write(p) // hash.Hash.Write never returns an error
	_, sw.err = sw.w.Write(p)
}

func (sw *snapshotWriter) writeBytes(p []byte) {
	sw.write(binary.AppendUvarint(sw.scratch[:0], uint64(len(p))))
	sw.write(p)
}

// LoadLatest removes leftover snapshot temp files and loads the newest
// snapshot in dir, returning its state and seq. With no snapshot it returns an
// empty Map and seq 0.
//
// If the newest snapshot is damaged, it returns ErrCorrupt instead of falling
// back to an older one. The WAL bridging the older snapshot to now may already
// be deleted, so a fallback could silently lose writes.
func LoadLatest(fsys vfs.FS, dir string) (*Map, uint64, error) {
	names, err := fsys.List(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("store: load snapshot: %w", err)
	}
	newest, found := uint64(0), false
	for _, name := range names {
		if base, isTemp := strings.CutSuffix(name, ".tmp"); isTemp {
			if _, ok := parseSnapshotName(base); ok {
				// A crash before the rename. Not dir-synced: if a crash undoes
				// the removal, the next load removes it again.
				if err := fsys.Remove(filepath.Join(dir, name)); err != nil {
					return nil, 0, fmt.Errorf("store: remove leftover %s: %w", name, err)
				}
			}
			continue
		}
		if seq, ok := parseSnapshotName(name); ok && (!found || seq > newest) {
			newest, found = seq, true
		}
	}
	if !found {
		return NewMap(), 0, nil
	}

	path := filepath.Join(dir, snapshotName(newest))
	data, err := vfs.ReadFile(fsys, path)
	if err != nil {
		return nil, 0, fmt.Errorf("store: load snapshot: %w", err)
	}
	m, seq, err := decodeSnapshot(data)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: %w", path, err)
	}
	if seq != newest {
		return nil, 0, fmt.Errorf("%w: %s: header seq %d doesn't match file name", ErrCorrupt, path, seq)
	}
	return m, seq, nil
}

// decodeSnapshot verifies and parses a whole snapshot file.
func decodeSnapshot(data []byte) (*Map, uint64, error) {
	if len(data) < snapshotHeaderSize+snapshotTrailerSize {
		return nil, 0, fmt.Errorf("%w: %d bytes, too short", ErrCorrupt, len(data))
	}
	body := data[:len(data)-snapshotTrailerSize]
	// Check the checksum before parsing anything, so no field is trusted unchecked.
	if stored, computed := binary.LittleEndian.Uint32(data[len(body):]), crc32.Checksum(body, castagnoli); stored != computed {
		return nil, 0, fmt.Errorf("%w: checksum mismatch (stored %08x, computed %08x)", ErrCorrupt, stored, computed)
	}
	if magic := binary.LittleEndian.Uint32(body[0:4]); magic != snapshotMagic {
		return nil, 0, fmt.Errorf("%w: bad magic %08x", ErrCorrupt, magic)
	}
	if v := binary.LittleEndian.Uint16(body[4:6]); v != snapshotVersion {
		return nil, 0, fmt.Errorf("%w: unsupported version %d", ErrCorrupt, v)
	}
	if reserved := binary.LittleEndian.Uint16(body[6:8]); reserved != 0 {
		return nil, 0, fmt.Errorf("%w: reserved header field is %d", ErrCorrupt, reserved)
	}
	seq := binary.LittleEndian.Uint64(body[8:16])
	count := binary.LittleEndian.Uint64(body[16:24])

	// A matching checksum means these are the bytes the writer wrote. Parse
	// strictly anyway, so a writer bug can't produce a silently wrong map.
	// Don't preallocate by count: it's only trusted once the entries back it up.
	m := NewMap()
	p := body[snapshotHeaderSize:]
	var prev []byte
	for i := range count {
		key, rest, err := readLengthPrefixed(p)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: entry %d key: %w", ErrCorrupt, i, err)
		}
		value, rest, err := readLengthPrefixed(rest)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: entry %d value: %w", ErrCorrupt, i, err)
		}
		// Strictly ascending also rules out duplicate keys.
		if i > 0 && bytes.Compare(prev, key) >= 0 {
			return nil, 0, fmt.Errorf("%w: entry %d: keys not in strictly ascending order", ErrCorrupt, i)
		}
		m.Put(key, value) // copies, so the Map doesn't keep the file buffer alive
		prev, p = key, rest
	}
	if len(p) != 0 {
		return nil, 0, fmt.Errorf("%w: %d bytes after the last entry", ErrCorrupt, len(p))
	}
	return m, seq, nil
}

// readLengthPrefixed reads a uvarint length n followed by n bytes, returning
// those bytes (pointing into p) and the rest of p.
func readLengthPrefixed(p []byte) (data, rest []byte, err error) {
	n, k := binary.Uvarint(p)
	if k <= 0 {
		return nil, nil, errors.New("malformed length varint")
	}
	p = p[k:]
	if n > uint64(len(p)) {
		return nil, nil, fmt.Errorf("length %d exceeds remaining %d bytes", n, len(p))
	}
	return p[:n], p[n:], nil
}
