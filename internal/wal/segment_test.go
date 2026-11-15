package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs/crashfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

const testDir = "/wal"

func newCrashFS() *crashfs.FS {
	f := crashfs.New()
	f.Mkdir(testDir)
	return f
}

func mustCreate(t *testing.T, fsys *crashfs.FS, startSeq uint64) *Writer {
	t.Helper()
	w, err := CreateSegment(fsys, testDir, startSeq)
	if err != nil {
		t.Fatalf("CreateSegment(%d): %v", startSeq, err)
	}
	return w
}

// readSegment returns a segment file's header, the records that decode
// cleanly from the start, and whatever bytes follow them.
func readSegment(t *testing.T, fsys *crashfs.FS, startSeq uint64) (header []byte, recs []Record, rest []byte) {
	t.Helper()
	data := []byte(vfstest.ReadFile(t, fsys, filepath.Join(testDir, segmentName(startSeq))))
	if len(data) < segmentHeaderSize {
		t.Fatalf("segment is %d bytes, shorter than its header", len(data))
	}
	header, rest = data[:segmentHeaderSize], data[segmentHeaderSize:]
	for {
		r, n, err := DecodeRecord(rest)
		if err != nil {
			return header, recs, rest
		}
		recs = append(recs, r)
		rest = rest[n:]
	}
}

func TestCreateSegmentWritesHeader(t *testing.T) {
	fsys := newCrashFS()
	w := mustCreate(t, fsys, 7)
	defer closeWriter(t, w)
	if names := vfstest.List(t, fsys, testDir); !slices.Equal(names, []string{"wal-00000000000000000007.log"}) {
		t.Fatalf("List = %q", names)
	}
	header, recs, rest := readSegment(t, fsys, 7)
	if !bytes.Equal(header, encodeSegmentHeader(7)) || len(recs) != 0 || len(rest) != 0 {
		t.Fatalf("header %x, %d records, %d extra bytes; want header only", header, len(recs), len(rest))
	}
	if !bytes.HasPrefix(header, []byte("KVWL")) {
		t.Fatalf("header %x doesn't start with \"KVWL\"", header)
	}
	if w.NextSeq() != 7 {
		t.Fatalf("NextSeq = %d, want 7", w.NextSeq())
	}
}

func TestCreateSegmentRefusesExisting(t *testing.T) {
	fsys := newCrashFS()
	path := filepath.Join(testDir, segmentName(1))
	vfstest.WriteFile(t, fsys, path, "acknowledged data")
	if _, err := CreateSegment(fsys, testDir, 1); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("err = %v, want fs.ErrExist", err)
	}
	if got := vfstest.ReadFile(t, fsys, path); got != "acknowledged data" {
		t.Fatalf("existing segment changed to %q", got)
	}
	if names := vfstest.List(t, fsys, testDir); len(names) != 1 {
		t.Fatalf("List = %q, want only the existing segment", names)
	}
}

// crashAtOp installs a hook that crashes fsys (with policy) just before its
// k-th operation from now (k = 1, 2, ...).
func crashAtOp(fsys *crashfs.FS, k int, policy crashfs.Policy) {
	n := 0
	fsys.SetHook(func(string, string) error {
		if n++; n == k {
			fsys.Crash(policy)
			return crashfs.ErrCrashed
		}
		return nil
	})
}

// Crash CreateSegment before each of its filesystem operations in turn (and
// once after it completes), under both policies. Whatever survives, a segment
// name must never point to a file without a complete, valid header.
func TestCreateSegmentCrashAtEveryOp(t *testing.T) {
	for k := 1; ; k++ {
		for _, policy := range []crashfs.Policy{crashfs.DropUnsynced(), crashfs.Random(rand.New(rand.NewPCG(uint64(k), 0)))} {
			fsys := newCrashFS()
			crashAtOp(fsys, k, policy)
			w, err := CreateSegment(fsys, testDir, 1)
			fsys.SetHook(nil)
			completed := err == nil
			if completed {
				closeWriter(t, w)
				fsys.Crash(policy)
			}

			names := vfstest.List(t, fsys, testDir)
			for _, name := range names {
				if name != segmentName(1) && name != segmentName(1)+".tmp" {
					t.Fatalf("crash at op %d: unexpected file %q", k, name)
				}
			}
			hasSegment := slices.Contains(names, segmentName(1))
			if hasSegment {
				header, recs, rest := readSegment(t, fsys, 1)
				if !bytes.Equal(header, encodeSegmentHeader(1)) || len(recs) != 0 || len(rest) != 0 {
					t.Fatalf("crash at op %d: segment holds %x + %d records + %d bytes; want exactly a valid header", k, header, len(recs), len(rest))
				}
			}
			if completed && !hasSegment {
				t.Fatalf("CreateSegment returned success, but the segment didn't survive a crash")
			}
			if completed && policy == crashfs.DropUnsynced() {
				return // k is past the last operation; every crash point has been covered
			}
		}
	}
}

func TestAckedAppendsSurviveCrash(t *testing.T) {
	fsys := newCrashFS()
	w := mustCreate(t, fsys, 1)
	var acked []Record
	for i := range 50 {
		r := Record{Op: OpPut, Key: fmt.Appendf(nil, "k%d", i), Value: bytes.Repeat([]byte{byte(i)}, i)}
		seq, err := w.Append(r.Op, r.Key, r.Value)
		if err != nil {
			t.Fatal(err)
		}
		r.Seq = seq
		acked = append(acked, r)
	}
	fsys.Crash(crashfs.DropUnsynced())
	_, recs, rest := readSegment(t, fsys, 1)
	if len(recs) != len(acked) || len(rest) != 0 {
		t.Fatalf("after crash: %d records + %d bytes, want %d records", len(recs), len(rest), len(acked))
	}
	for i := range acked {
		if !sameRecord(recs[i], acked[i]) {
			t.Fatalf("record %d = %+v, want %+v", i, recs[i], acked[i])
		}
	}
}

// Crash at a random operation in a stream of appends, keeping a random part of
// the unsynced bytes. Every acknowledged record must survive intact. The one
// in-flight record may survive whole (allowed: it was never acknowledged), or
// leave a torn prefix of itself, possibly with zeroed sectors. Nothing else
// may appear.
func TestCrashDuringAppendsKeepsAckedPrefix(t *testing.T) {
	for seed := range uint64(100) {
		rng := rand.New(rand.NewPCG(seed, 0))
		fsys := newCrashFS()
		w := mustCreate(t, fsys, 1)
		crashAtOp(fsys, 1+rng.IntN(120), crashfs.Random(rng)) // each append = write + sync

		var acked []Record
		var inflight []byte // encoding of the record being appended when the crash hit
		for i := 0; ; i++ {
			r := Record{Seq: w.NextSeq(), Op: OpPut, Key: fmt.Appendf(nil, "key-%d", i), Value: bytes.Repeat([]byte("v"), rng.IntN(1500))}
			inflight = mustEncode(t, r)
			if _, err := w.Append(r.Op, r.Key, r.Value); err != nil {
				break
			}
			acked = append(acked, r)
		}
		fsys.SetHook(nil)

		_, recs, rest := readSegment(t, fsys, 1)
		if len(recs) < len(acked) {
			t.Fatalf("seed %d: %d acknowledged records, only %d survived", seed, len(acked), len(recs))
		}
		for i := range acked {
			if !sameRecord(recs[i], acked[i]) {
				t.Fatalf("seed %d: record %d = %+v, want %+v", seed, i, recs[i], acked[i])
			}
		}
		switch {
		case len(recs) == len(acked)+1:
			if enc := mustEncode(t, recs[len(acked)]); !bytes.Equal(enc, inflight) || len(rest) != 0 {
				t.Fatalf("seed %d: extra record isn't the in-flight one, or bytes follow it", seed)
			}
		case len(recs) > len(acked)+1:
			t.Fatalf("seed %d: %d records survived, but only %d were acked plus 1 in flight", seed, len(recs), len(acked))
		default:
			// The tail can be the in-flight record's full length yet fail to
			// decode, when a zeroed sector lands inside it (seed 58).
			if len(rest) > len(inflight) {
				t.Fatalf("seed %d: %d trailing bytes, longer than the in-flight record (%d)", seed, len(rest), len(inflight))
			}
			for i, b := range rest {
				if b != inflight[i] && b != 0 {
					t.Fatalf("seed %d: trailing byte %d is neither the in-flight record's nor zero", seed, i)
				}
			}
		}
	}
}

func TestIOErrorFailsWriterForever(t *testing.T) {
	for _, failOp := range []string{"write", "sync"} {
		t.Run(failOp, func(t *testing.T) {
			fsys := newCrashFS()
			w := mustCreate(t, fsys, 1)
			defer closeWriter(t, w)
			errDisk := errors.New("injected EIO")
			fsys.SetHook(func(op, _ string) error {
				if op == failOp {
					return errDisk
				}
				return nil
			})
			_, err := w.Append(OpPut, []byte("k"), []byte("v"))
			if !errors.Is(err, ErrFailed) || !errors.Is(err, errDisk) {
				t.Fatalf("failing Append: err = %v, want ErrFailed wrapping the injected error", err)
			}
			fsys.SetHook(nil) // the disk "recovers", but the writer must not trust it
			if _, err := w.Append(OpPut, []byte("k"), []byte("v")); !errors.Is(err, ErrFailed) {
				t.Fatalf("Append after failure: err = %v, want ErrFailed", err)
			}
		})
	}
}

func TestInvalidRecordDoesNotFailWriter(t *testing.T) {
	fsys := newCrashFS()
	w := mustCreate(t, fsys, 5)
	defer closeWriter(t, w)
	if _, err := w.Append(OpPut, nil, []byte("v")); !errors.Is(err, ErrInvalidRecord) || errors.Is(err, ErrFailed) {
		t.Fatalf("empty key: err = %v, want ErrInvalidRecord only", err)
	}
	seq, err := w.Append(OpPut, []byte("k"), []byte("v"))
	if err != nil || seq != 5 {
		t.Fatalf("next Append = %d, %v; want seq 5 (none used up by the rejected record)", seq, err)
	}
	if _, recs, rest := readSegment(t, fsys, 5); len(recs) != 1 || len(rest) != 0 {
		t.Fatalf("segment holds %d records + %d bytes, want exactly 1 record", len(recs), len(rest))
	}
}

func closeWriter(t *testing.T, w *Writer) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
