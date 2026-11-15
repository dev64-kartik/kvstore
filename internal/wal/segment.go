package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"

	"github.com/dev64-kartik/kvstore/internal/vfs"
)

// Segment file header. All integers are little-endian.
//
//	offset 0   magic    u32  0x4C57564B, which is the bytes "KVWL" on disk
//	offset 4   version  u16  1
//	offset 6   reserved u16  0
//	offset 8   startSeq u64  seq of the first record in this segment
//
// The header has no checksum of its own. The segment's file name also encodes
// startSeq, and the reader checks that the two match.
const (
	segmentMagic      uint32 = 0x4C57564B
	segmentVersion    uint16 = 1
	segmentHeaderSize        = 16
)

// ErrFailed means an earlier Write or Sync failed, so the segment's on-disk
// state is unknown. The writer refuses all further appends. The only way
// forward is to reopen the WAL, which runs recovery (design doc: "Write
// failure policy").
var ErrFailed = errors.New("wal: writer failed; reopen to recover")

// segmentName is zero-padded to 20 digits (the width of the largest uint64),
// so sorting names as strings sorts segments by startSeq.
func segmentName(startSeq uint64) string {
	return fmt.Sprintf("wal-%020d.log", startSeq)
}

func encodeSegmentHeader(startSeq uint64) []byte {
	h := make([]byte, segmentHeaderSize)
	binary.LittleEndian.PutUint32(h[0:4], segmentMagic)
	binary.LittleEndian.PutUint16(h[4:6], segmentVersion)
	binary.LittleEndian.PutUint64(h[8:16], startSeq)
	return h
}

// Writer appends records to one segment file. It is not safe for concurrent
// use: localdb's writer slot serializes callers.
type Writer struct {
	f       vfs.File
	path    string
	nextSeq uint64
	buf     []byte // reused encode buffer; it grows to the largest record seen
	failed  error  // the first Write or Sync error, kept forever once set
}

// CreateSegment durably creates an empty segment whose first record will
// have seq startSeq, and returns a Writer for it.
//
// If it returns an error, a .tmp file or even the renamed segment (holding
// only a header) may be left in dir. Recovery removes .tmp files, and a
// header-only segment is valid and empty.
func CreateSegment(fsys vfs.FS, dir string, startSeq uint64) (_ *Writer, err error) {
	if startSeq == 0 {
		return nil, errors.New("wal: startSeq must be at least 1")
	}
	name := segmentName(startSeq)
	final := filepath.Join(dir, name)
	tmp := final + ".tmp"

	// Rename silently replaces its target. Without this check, a stale
	// startSeq would replace a live segment, and the acknowledged writes in it,
	// with an empty one. This process owns dir, so nothing can create the file
	// between this check and the rename.
	names, err := fsys.List(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: create segment: %w", err)
	}
	if slices.Contains(names, name) {
		return nil, &fs.PathError{Op: "create segment", Path: final, Err: fs.ErrExist}
	}

	f, err := fsys.Create(tmp)
	if err != nil {
		return nil, fmt.Errorf("wal: create segment: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()

	if _, err := f.Write(encodeSegmentHeader(startSeq)); err != nil {
		return nil, fmt.Errorf("wal: write segment header %s: %w", tmp, err)
	}
	// Sync before the rename: then any name a crash leaves visible points to a
	// file with a complete header, so a bad header always means corruption,
	// never a torn write.
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("wal: sync segment header %s: %w", tmp, err)
	}
	if err := fsys.Rename(tmp, final); err != nil {
		return nil, fmt.Errorf("wal: rename segment: %w", err)
	}
	// Sync the directory before any record is acknowledged into this file.
	// Until the name is durable, a crash can lose the whole file, and with it
	// every write we'd already acknowledged.
	if err := fsys.SyncDir(dir); err != nil {
		return nil, fmt.Errorf("wal: sync dir after creating %s: %w", final, err)
	}
	// The handle stays open across the rename (it refers to the file, not the
	// name) and is positioned just after the header.
	return &Writer{f: f, path: final, nextSeq: startSeq}, nil
}

// NextSeq is the seq the next successful Append will be given.
func (w *Writer) NextSeq() uint64 {
	return w.nextSeq
}

// Append durably logs one mutation and returns its seq. When it returns nil,
// the record is on disk (Write followed by Sync).
//
// Seqs are assigned here, never by the caller, so a gap or duplicate can't be
// introduced. On ErrInvalidRecord nothing was written and no seq is used up.
// On any I/O error the writer is permanently failed, and this and every later
// call return ErrFailed.
func (w *Writer) Append(op Op, key, value []byte) (uint64, error) {
	if w.failed != nil {
		return 0, fmt.Errorf("%w: %w", ErrFailed, w.failed)
	}
	seq := w.nextSeq
	buf, err := AppendRecord(w.buf[:0], Record{Seq: seq, Op: op, Key: key, Value: value})
	if err != nil {
		return 0, err // nothing reached the file, so it is still clean
	}
	w.buf = buf

	// One Write call per record. A crash can then tear at most this record,
	// which is the invariant recovery's torn-tail rule depends on.
	if _, err := w.f.Write(buf); err != nil {
		return 0, w.fail("write", err)
	}
	if err := w.f.Sync(); err != nil {
		// Don't retry: after a failed fsync, a retry can report success without
		// the data being on disk (design doc: "Write failure policy").
		return 0, w.fail("sync", err)
	}
	w.nextSeq++
	return seq, nil
}

func (w *Writer) fail(op string, err error) error {
	w.failed = fmt.Errorf("%s %s: %w", op, w.path, err)
	return fmt.Errorf("%w: %w", ErrFailed, w.failed)
}

// Close releases the file. It doesn't sync, because every successful Append
// already did. Close works on a failed writer too, and must be called once.
func (w *Writer) Close() error {
	return w.f.Close()
}
