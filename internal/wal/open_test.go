package wal

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs/crashfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

func cloneRecord(r Record) Record {
	return Record{Seq: r.Seq, Op: r.Op, Key: bytes.Clone(r.Key), Value: bytes.Clone(r.Value)}
}

// openWAL runs Open and collects the records it replays (copied, since they
// point into Open's read buffer).
func openWAL(fsys *crashfs.FS, afterSeq uint64) (*Writer, []Record, OpenInfo, error) {
	var recs []Record
	w, info, err := Open(fsys, testDir, afterSeq, func(r Record) error {
		recs = append(recs, cloneRecord(r))
		return nil
	})
	return w, recs, info, err
}

func seqsOf(recs []Record) []uint64 {
	var s []uint64
	for _, r := range recs {
		s = append(s, r.Seq)
	}
	return s
}

// writeSegment creates a segment holding n records with seqs startSeq.. and
// closes it.
func writeSegment(t *testing.T, fsys *crashfs.FS, startSeq uint64, n int) {
	t.Helper()
	w := mustCreate(t, fsys, startSeq)
	for i := range n {
		seq := startSeq + uint64(i)
		if _, err := w.Append(OpPut, fmt.Appendf(nil, "k%d", seq), fmt.Appendf(nil, "v%d", seq)); err != nil {
			t.Fatal(err)
		}
	}
	closeWriter(t, w)
}

func segPath(startSeq uint64) string { return filepath.Join(testDir, segmentName(startSeq)) }

// rewrite replaces a file's contents with edit(contents), durably.
func rewrite(t *testing.T, fsys *crashfs.FS, path string, edit func([]byte) []byte) {
	t.Helper()
	data := edit([]byte(vfstest.ReadFile(t, fsys, path)))
	if err := fsys.Remove(path); err != nil {
		t.Fatal(err)
	}
	vfstest.WriteFile(t, fsys, path, string(data))
	if err := fsys.SyncDir(testDir); err != nil {
		t.Fatal(err)
	}
}

func TestOpenFreshDir(t *testing.T) {
	fsys := newCrashFS()
	w, recs, _, err := openWAL(fsys, 0)
	if err != nil || len(recs) != 0 || w.NextSeq() != 1 {
		t.Fatalf("Open on empty dir: %d records, err %v", len(recs), err)
	}
	if _, err := w.Append(OpPut, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	closeWriter(t, w)
	w, recs, _, err = openWAL(fsys, 0)
	if err != nil || !slices.Equal(seqsOf(recs), []uint64{1}) || w.NextSeq() != 2 {
		t.Fatalf("reopen: seqs %v, err %v", seqsOf(recs), err)
	}
	closeWriter(t, w)
}

func TestOpenReplaysAcrossSegments(t *testing.T) {
	fsys := newCrashFS()
	writeSegment(t, fsys, 1, 3) // 1..3
	writeSegment(t, fsys, 4, 2) // 4..5
	writeSegment(t, fsys, 6, 1) // 6, the last segment

	for afterSeq, want := range map[uint64][]uint64{0: {1, 2, 3, 4, 5, 6}, 4: {5, 6}, 6: nil} {
		w, recs, _, err := openWAL(fsys, afterSeq)
		if err != nil || !slices.Equal(seqsOf(recs), want) || w.NextSeq() != 7 {
			t.Fatalf("afterSeq %d: seqs %v, err %v; want %v", afterSeq, seqsOf(recs), err, want)
		}
		if len(recs) > 0 && string(recs[len(recs)-1].Value) != "v6" {
			t.Fatalf("afterSeq %d: last value %q", afterSeq, recs[len(recs)-1].Value)
		}
		closeWriter(t, w)
	}
}

// A segment wholly inside the snapshot isn't read, so damage there (in data
// GC is about to delete anyway) doesn't block startup.
func TestOpenSkipsSegmentsCoveredBySnapshot(t *testing.T) {
	fsys := newCrashFS()
	writeSegment(t, fsys, 1, 3)
	writeSegment(t, fsys, 4, 2)
	rewrite(t, fsys, segPath(1), func(b []byte) []byte { b[segmentHeaderSize+headerSize] ^= 0xff; return b })

	if _, _, _, err := openWAL(fsys, 0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("afterSeq 0 reads the damaged segment: err = %v, want ErrCorrupt", err)
	}
	w, recs, _, err := openWAL(fsys, 3)
	if err != nil || !slices.Equal(seqsOf(recs), []uint64{4, 5}) {
		t.Fatalf("afterSeq 3: seqs %v, err %v; want [4 5]", seqsOf(recs), err)
	}
	closeWriter(t, w)
}

func TestOpenRepairsTornTail(t *testing.T) {
	fsys := newCrashFS()
	writeSegment(t, fsys, 1, 3)
	validLen := len(vfstest.ReadFile(t, fsys, segPath(1)))
	torn := mustEncode(t, Record{Seq: 4, Op: OpPut, Key: []byte("k"), Value: []byte("half-written")})
	torn = torn[:len(torn)/2]
	rewrite(t, fsys, segPath(1), func(b []byte) []byte { return append(b, torn...) })

	w, recs, info, err := openWAL(fsys, 0)
	if err != nil || !slices.Equal(seqsOf(recs), []uint64{1, 2, 3}) || info.TornBytes != len(torn) {
		t.Fatalf("seqs %v, info %+v, err %v; want 1..3 and %d torn bytes", seqsOf(recs), info, err, len(torn))
	}
	if got := len(vfstest.ReadFile(t, fsys, segPath(1))); got != validLen {
		t.Fatalf("file is %d bytes after repair, want %d", got, validLen)
	}
	if seq, err := w.Append(OpPut, []byte("k"), []byte("after repair")); err != nil || seq != 4 {
		t.Fatalf("Append after repair = %d, %v; want seq 4", seq, err)
	}

	// The repair and the new record are durable: a crash right now changes nothing.
	fsys.Crash(crashfs.DropUnsynced())
	w, recs, info, err = openWAL(fsys, 0)
	if err != nil || !slices.Equal(seqsOf(recs), []uint64{1, 2, 3, 4}) || info.TornBytes != 0 {
		t.Fatalf("after crash: seqs %v, info %+v, err %v", seqsOf(recs), info, err)
	}
	if string(recs[3].Value) != "after repair" {
		t.Fatalf("seq 4 = %q, want the record appended after repair", recs[3].Value)
	}
	closeWriter(t, w)
}

func TestOpenRejectsBrokenLayouts(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T, fsys *crashfs.FS)
		afterSeq uint64
	}{
		{"gap before the last segment", func(t *testing.T, f *crashfs.FS) { writeSegment(t, f, 1, 3); writeSegment(t, f, 5, 1) }, 0},
		// Needs three segments: a gap between two sealed ones is a separate
		// check from the gap before the last segment.
		{"gap between sealed segments", func(t *testing.T, f *crashfs.FS) {
			writeSegment(t, f, 1, 3)
			writeSegment(t, f, 5, 1)
			writeSegment(t, f, 6, 1)
		}, 0},
		{"oldest segment starts after the snapshot", func(t *testing.T, f *crashfs.FS) { writeSegment(t, f, 4, 2) }, 0},
		{"log ends before the snapshot", func(t *testing.T, f *crashfs.FS) { writeSegment(t, f, 1, 3) }, 5},
		{"no segments, but a snapshot", func(*testing.T, *crashfs.FS) {}, 2},
		{"torn tail in a sealed segment", func(t *testing.T, f *crashfs.FS) {
			writeSegment(t, f, 1, 3)
			rewrite(t, f, segPath(1), func(b []byte) []byte { return b[:len(b)-1] })
			writeSegment(t, f, 3, 1)
		}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := newCrashFS()
			tt.setup(t, fsys)
			before := vfstest.List(t, fsys, testDir)
			if _, _, _, err := openWAL(fsys, tt.afterSeq); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
			if after := vfstest.List(t, fsys, testDir); !slices.Equal(before, after) {
				t.Fatalf("a failed Open changed the directory: %q -> %q", before, after)
			}
		})
	}
}

func TestOpenRemovesTempSegmentsAndIgnoresStrangers(t *testing.T) {
	fsys := newCrashFS()
	// A leftover from a crash inside CreateSegment(1). Because CreateSegment
	// uses O_EXCL, it would block creating segment 1 forever if not removed.
	vfstest.WriteFile(t, fsys, segPath(1)+".tmp", "partial header")
	vfstest.WriteFile(t, fsys, filepath.Join(testDir, "snap-00000000000000000009.snap"), "not ours")
	vfstest.WriteFile(t, fsys, filepath.Join(testDir, "wal-1.log"), "not a canonical name")
	if err := fsys.SyncDir(testDir); err != nil {
		t.Fatal(err)
	}

	w, recs, _, err := openWAL(fsys, 0)
	if err != nil || len(recs) != 0 {
		t.Fatalf("Open: %d records, err %v", len(recs), err)
	}
	closeWriter(t, w)
	want := []string{"snap-00000000000000000009.snap", "wal-00000000000000000001.log", "wal-1.log"}
	if names := vfstest.List(t, fsys, testDir); !slices.Equal(names, want) {
		t.Fatalf("List = %q, want %q", names, want)
	}
}

// Repeated cycles of: open (which may itself crash), append, crash with a
// random policy. After every successful recovery:
//   - everything durable before the crash comes back unchanged, in order;
//   - at most one more record appears, and only the one in flight at the crash;
//   - what came back stays durable: the next recovery starts from it.
func TestCrashRecoverAppendCycles(t *testing.T) {
	for seed := range uint64(60) {
		rng := rand.New(rand.NewPCG(seed, 0))
		fsys := newCrashFS()
		var durable []Record // what the last recovery returned, plus acknowledged appends since
		var inflight *Record // the record being appended when the crash hit

		for cycle := range 8 {
			crashAtOp(fsys, 1+rng.IntN(30), crashfs.Random(rng))
			w, recs, _, err := openWAL(fsys, 0)
			if err != nil {
				if !errors.Is(err, crashfs.ErrCrashed) {
					t.Fatalf("seed %d cycle %d: Open: %v", seed, cycle, err)
				}
				fsys.SetHook(nil) // crashed during recovery; recover again next cycle
				continue
			}

			if len(recs) < len(durable) || len(recs) > len(durable)+1 {
				t.Fatalf("seed %d cycle %d: recovered %d records, had %d durable", seed, cycle, len(recs), len(durable))
			}
			for i := range durable {
				if !sameRecord(recs[i], durable[i]) {
					t.Fatalf("seed %d cycle %d: record %d = %+v, want %+v", seed, cycle, i, recs[i], durable[i])
				}
			}
			if len(recs) == len(durable)+1 && (inflight == nil || !sameRecord(recs[len(durable)], *inflight)) {
				t.Fatalf("seed %d cycle %d: extra record %+v isn't the in-flight one", seed, cycle, recs[len(durable)])
			}
			durable, inflight = recs, nil

			crashed := false
			for i := range rng.IntN(12) {
				r := Record{Seq: w.NextSeq(), Op: OpPut, Key: fmt.Appendf(nil, "s%d-c%d-%d", seed, cycle, i), Value: bytes.Repeat([]byte{'x'}, rng.IntN(1200))}
				inflight = &r
				if _, err := w.Append(r.Op, r.Key, r.Value); err != nil {
					crashed = true
					break
				}
				durable, inflight = append(durable, r), nil
			}
			if !crashed {
				fsys.Crash(crashfs.Random(rng))
			}
			fsys.SetHook(nil)
		}
	}
}
