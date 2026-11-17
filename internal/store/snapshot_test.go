package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/crashfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

const snapDir = "/data"

func newSnapFS() *crashfs.FS {
	f := crashfs.New()
	f.Mkdir(snapDir)
	return f
}

// dump renders a Map's contents in order, for comparing two maps.
func dump(m *Map) string {
	var b strings.Builder
	m.Ascend(func(k, v []byte) bool {
		fmt.Fprintf(&b, "%q=%q;", k, v)
		return true
	})
	return b.String()
}

func mapOf(pairs ...string) *Map {
	m := NewMap()
	for i := 0; i+1 < len(pairs); i += 2 {
		m.Put([]byte(pairs[i]), []byte(pairs[i+1]))
	}
	return m
}

func mustWrite(t *testing.T, fsys vfs.FS, seq uint64, m *Map) {
	t.Helper()
	if err := WriteSnapshot(context.Background(), fsys, snapDir, seq, m); err != nil {
		t.Fatalf("WriteSnapshot(%d): %v", seq, err)
	}
}

func mustLoad(t *testing.T, fsys vfs.FS) (*Map, uint64) {
	t.Helper()
	m, seq, err := LoadLatest(fsys, snapDir)
	if err != nil {
		t.Fatalf("LoadLatest: %v", err)
	}
	return m, seq
}

func snapBytes(t *testing.T, fsys vfs.FS, seq uint64) []byte {
	t.Helper()
	return []byte(vfstest.ReadFile(t, fsys, filepath.Join(snapDir, snapshotName(seq))))
}

func TestSnapshotRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(testSeed(t), 0))
	big := NewMap()
	for range 5000 {
		big.Put(randKey(rng), randValue(rng))
		big.Put(fmt.Appendf(nil, "k%d", rng.IntN(1e6)), randValue(rng))
	}
	for name, m := range map[string]*Map{
		"empty":       NewMap(),
		"empty value": mapOf("k", ""),
		"binary keys": mapOf("\x00", "a", "\xff", "b", "", "empty key"),
		"random":      big,
	} {
		fsys := newSnapFS()
		mustWrite(t, fsys, 42, m)
		got, seq := mustLoad(t, fsys)
		if seq != 42 || dump(got) != dump(m) {
			t.Fatalf("%s: seq %d, contents match: %v", name, seq, dump(got) == dump(m))
		}
	}
}

func TestLoadLatestWithNoSnapshot(t *testing.T) {
	m, seq := mustLoad(t, newSnapFS())
	if seq != 0 || m.Len() != 0 {
		t.Fatalf("seq %d, %d keys; want 0, empty", seq, m.Len())
	}
}

// The same state, built in different orders, produces identical bytes.
func TestSnapshotBytesAreDeterministic(t *testing.T) {
	a, b := newSnapFS(), newSnapFS()
	mustWrite(t, a, 1, mapOf("x", "1", "a", "2", "m", "3"))
	mb := mapOf("m", "old", "a", "2")
	mb.Put([]byte("x"), []byte("1"))
	mb.Put([]byte("m"), []byte("3"))
	mustWrite(t, b, 1, mb)
	if !slices.Equal(snapBytes(t, a, 1), snapBytes(t, b, 1)) {
		t.Fatal("same contents, different snapshot bytes")
	}
}

func TestSnapshotDetectsEveryBitFlipAndTruncation(t *testing.T) {
	fsys := newSnapFS()
	mustWrite(t, fsys, 3, mapOf("alpha", "1", "beta", "22", "gamma", ""))
	orig := snapBytes(t, fsys, 3)
	for bit := range len(orig) * 8 {
		data := slices.Clone(orig)
		data[bit/8] ^= 1 << (bit % 8)
		if _, _, err := decodeSnapshot(data); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("bit %d flipped: err = %v, want ErrCorrupt", bit, err)
		}
	}
	for n := range len(orig) {
		if _, _, err := decodeSnapshot(orig[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("first %d bytes: err = %v, want ErrCorrupt", n, err)
		}
	}
}

// seal appends a correct checksum, to build files that pass the CRC check but
// are malformed, as a buggy writer would produce.
func seal(body []byte) []byte {
	return binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, castagnoli))
}

func encEntry(k, v string) []byte {
	b := binary.AppendUvarint(nil, uint64(len(k)))
	b = append(b, k...)
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func TestSnapshotMalformedWithValidChecksum(t *testing.T) {
	cat := func(parts ...[]byte) []byte { return slices.Concat(parts...) }
	badHeader := func(off int, val byte) []byte { h := snapshotHeader(1, 0); h[off] = val; return h }
	tests := map[string][]byte{
		"bad magic":           badHeader(0, 'X'),
		"future version":      badHeader(4, 2),
		"reserved set":        badHeader(6, 1),
		"keys out of order":   cat(snapshotHeader(1, 2), encEntry("b", "1"), encEntry("a", "2")),
		"duplicate key":       cat(snapshotHeader(1, 2), encEntry("a", "1"), encEntry("a", "2")),
		"count too high":      cat(snapshotHeader(1, 3), encEntry("a", "1"), encEntry("b", "2")),
		"count too low":       cat(snapshotHeader(1, 1), encEntry("a", "1"), encEntry("b", "2")),
		"huge count":          cat(snapshotHeader(1, 1<<62), encEntry("a", "1")),
		"key length past end": cat(snapshotHeader(1, 1), []byte{50, 'a'}),
	}
	for name, body := range tests {
		if _, _, err := decodeSnapshot(seal(body)); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
	}
}

func TestLoadLatestPicksNewestAndCleansUp(t *testing.T) {
	fsys := newSnapFS()
	mustWrite(t, fsys, 5, mapOf("v", "five"))
	mustWrite(t, fsys, 12, mapOf("v", "twelve"))
	mustWrite(t, fsys, 9, mapOf("v", "nine"))
	leftover := filepath.Join(snapDir, snapshotName(20)+".tmp")
	vfstest.WriteFile(t, fsys, leftover, "crashed before rename")
	vfstest.WriteFile(t, fsys, filepath.Join(snapDir, "wal-00000000000000000001.log"), "not ours")

	m, seq := mustLoad(t, fsys)
	if seq != 12 || dump(m) != dump(mapOf("v", "twelve")) {
		t.Fatalf("loaded seq %d %s, want 12", seq, dump(m))
	}
	if slices.Contains(vfstest.List(t, fsys, snapDir), filepath.Base(leftover)) {
		t.Fatal("leftover snapshot temp file not removed")
	}
}

// No silent fallback: older snapshots are fine, but the newest is damaged.
func TestLoadLatestDoesNotFallBack(t *testing.T) {
	fsys := newSnapFS()
	mustWrite(t, fsys, 5, mapOf("v", "five"))
	mustWrite(t, fsys, 9, mapOf("v", "nine"))
	path := filepath.Join(snapDir, snapshotName(9))
	data := snapBytes(t, fsys, 9)
	data[snapshotHeaderSize+1] ^= 0x01
	if err := fsys.Remove(path); err != nil {
		t.Fatal(err)
	}
	vfstest.WriteFile(t, fsys, path, string(data))

	if _, _, err := LoadLatest(fsys, snapDir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt (not the seq-5 snapshot)", err)
	}
}

func TestLoadLatestRejectsNameHeaderMismatch(t *testing.T) {
	fsys := newSnapFS()
	mustWrite(t, fsys, 5, mapOf("k", "v"))
	if err := fsys.Rename(filepath.Join(snapDir, snapshotName(5)), filepath.Join(snapDir, snapshotName(6))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadLatest(fsys, snapDir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestWriteSnapshotRefusesExisting(t *testing.T) {
	fsys := newSnapFS()
	mustWrite(t, fsys, 5, mapOf("v", "original"))
	if err := WriteSnapshot(context.Background(), fsys, snapDir, 5, mapOf("v", "replacement")); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("err = %v, want fs.ErrExist", err)
	}
	if m, _ := mustLoad(t, fsys); dump(m) != dump(mapOf("v", "original")) {
		t.Fatalf("existing snapshot replaced: %s", dump(m))
	}
}

// Crash before each filesystem operation of WriteSnapshot in turn (and once
// after it completes), under both policies. Recovery must see the old snapshot
// or the new one, complete, never a partial or corrupt one. Once
// WriteSnapshot has returned nil, only the new one.
func TestWriteSnapshotCrashAtEveryOp(t *testing.T) {
	oldMap, newMap := mapOf("v", "old"), mapOf("v", "new", "w", "added")
	for k := 1; ; k++ {
		for _, policy := range []crashfs.Policy{crashfs.DropUnsynced(), crashfs.Random(rand.New(rand.NewPCG(uint64(k), 0)))} {
			fsys := newSnapFS()
			mustWrite(t, fsys, 5, oldMap)
			n := 0
			fsys.SetHook(func(string, string) error {
				if n++; n == k {
					fsys.Crash(policy)
					return crashfs.ErrCrashed
				}
				return nil
			})
			err := WriteSnapshot(context.Background(), fsys, snapDir, 9, newMap)
			fsys.SetHook(nil)
			completed := err == nil
			if completed {
				fsys.Crash(policy)
			}

			m, seq, err := LoadLatest(fsys, snapDir)
			switch {
			case err != nil:
				t.Fatalf("crash at op %d: LoadLatest: %v", k, err)
			case completed && (seq != 9 || dump(m) != dump(newMap)):
				t.Fatalf("crash after success: loaded seq %d %s, want the new snapshot", seq, dump(m))
			case seq == 5 && dump(m) != dump(oldMap), seq == 9 && dump(m) != dump(newMap), seq != 5 && seq != 9:
				t.Fatalf("crash at op %d: loaded seq %d %s, want old or new intact", k, seq, dump(m))
			}
			if completed && policy == crashfs.DropUnsynced() {
				return // k is past the last operation; every crash point has been covered
			}
		}
	}
}

// A failed or cancelled snapshot leaves nothing behind, so a retry works.
func TestWriteSnapshotFailureCleansUp(t *testing.T) {
	errDisk := errors.New("injected EIO")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := map[string]struct {
		ctx     context.Context
		failOp  string
		wantErr error
	}{
		"sync fails":    {context.Background(), "sync", errDisk},
		"write fails":   {context.Background(), "write", errDisk},
		"ctx cancelled": {cancelled, "", context.Canceled},
		"syncdir fails": {context.Background(), "syncdir", errDisk},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fsys := newSnapFS()
			fsys.SetHook(func(op, _ string) error {
				if op == tt.failOp {
					return errDisk
				}
				return nil
			})
			if err := WriteSnapshot(tt.ctx, fsys, snapDir, 7, mapOf("k", "v")); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			fsys.SetHook(nil)
			names := vfstest.List(t, fsys, snapDir)
			if slices.Contains(names, snapshotName(7)+".tmp") {
				t.Fatalf("temp file left behind: %q", names)
			}
			if tt.failOp == "syncdir" {
				return // the rename happened; only its durability is in doubt
			}
			mustWrite(t, fsys, 7, mapOf("k", "v")) // retry succeeds
		})
	}
}

func FuzzDecodeSnapshot(f *testing.F) {
	fsys := newSnapFS()
	if err := WriteSnapshot(context.Background(), fsys, snapDir, 1, mapOf("a", "1", "b", "")); err != nil {
		f.Fatal(err)
	}
	data, err := vfs.ReadFile(fsys, filepath.Join(snapDir, snapshotName(1)))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
	f.Add(seal(snapshotHeader(1, 0)))
	f.Fuzz(func(t *testing.T, data []byte) {
		m, _, err := decodeSnapshot(data)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("unexpected error kind: %v", err)
			}
			return
		}
		if m.Len() != int(binary.LittleEndian.Uint64(data[16:24])) {
			t.Fatalf("decoded %d entries, header says %d", m.Len(), binary.LittleEndian.Uint64(data[16:24]))
		}
	})
}

func TestRemoveOlder(t *testing.T) {
	fsys := newSnapFS()
	for _, seq := range []uint64{3, 7, 9} {
		mustWrite(t, fsys, seq, mapOf("k", "v"))
	}
	vfstest.WriteFile(t, fsys, filepath.Join(snapDir, "wal-00000000000000000001.log"), "not ours")
	removed, err := RemoveOlder(fsys, snapDir, 9)
	if err != nil || removed != 2 {
		t.Fatalf("RemoveOlder = %d, %v; want 2", removed, err)
	}
	want := []string{snapshotName(9), "wal-00000000000000000001.log"}
	if got := vfstest.List(t, fsys, snapDir); !slices.Equal(got, want) {
		t.Fatalf("left %q, want %q", got, want)
	}
}
