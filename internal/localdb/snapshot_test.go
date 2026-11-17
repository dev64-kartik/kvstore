package localdb

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs/crashfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

func seg(seq uint64) string  { return fmt.Sprintf("wal-%020d.log", seq) }
func snap(seq uint64) string { return fmt.Sprintf("snap-%020d.snap", seq) }

func wantFiles(t *testing.T, fsys *crashfs.FS, want ...string) {
	t.Helper()
	if got := vfstest.List(t, fsys, testDir); !slices.Equal(got, want) {
		t.Fatalf("files = %q, want %q", got, want)
	}
}

func mustSnapshot(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Snapshot(context.Background()); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
}

// failOnce makes the first filesystem op matching match return an injected
// error. after, if set, must have matched an earlier op first.
func failOnce(fsys *crashfs.FS, after, match func(op, name string) bool) {
	armed, done := after == nil, false
	fsys.SetHook(func(op, name string) error {
		switch {
		case done:
		case !armed:
			armed = after(op, name)
		case match(op, name):
			done = true
			return errors.New("injected EIO")
		}
		return nil
	})
}

func renameOf(suffix string) func(op, name string) bool {
	return func(op, name string) bool { return op == "rename" && strings.HasSuffix(name, suffix) }
}

func isOp(want string) func(op, name string) bool {
	return func(op, _ string) bool { return op == want }
}

func TestSnapshotReplacesOldFiles(t *testing.T) {
	fsys := newFS()
	db := mustOpen(t, fsys)
	ctx := context.Background()
	mustPut(t, db, "a", "1") // seq 1
	mustPut(t, db, "b", "2") // seq 2
	mustSnapshot(t, db)
	wantFiles(t, fsys, snap(2), seg(3))

	mustPut(t, db, "a", "3")                            // seq 3
	if err := db.Delete(ctx, []byte("b")); err != nil { // seq 4
		t.Fatal(err)
	}
	mustSnapshot(t, db)
	wantFiles(t, fsys, snap(4), seg(5))
	mustPut(t, db, "c", "5") // seq 5, only in the WAL

	// No Close: after a crash, the snapshot, the cleanup and seq 5 are all durable.
	fsys.Crash(crashfs.DropUnsynced())
	db = mustOpen(t, fsys)
	defer mustClose(t, db)
	wantFiles(t, fsys, snap(4), seg(5))
	wantGet(t, db, "a", []byte("3"))
	wantGet(t, db, "b", nil)
	wantGet(t, db, "c", []byte("5"))
}

// Edge case 1.37: with nothing new to snapshot, Snapshot must not try to
// create a segment that already exists.
func TestSnapshotWithNothingNewIsNoop(t *testing.T) {
	fsys := newFS()
	db := mustOpen(t, fsys)
	defer mustClose(t, db)
	mustSnapshot(t, db) // fresh DB: empty segment 1, no snapshot
	wantFiles(t, fsys, seg(1))

	mustPut(t, db, "k", "v")
	mustSnapshot(t, db)
	mustSnapshot(t, db) // nothing since the last one
	wantFiles(t, fsys, snap(1), seg(2))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.Snapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Snapshot with cancelled ctx: %v", err)
	}
	mustPut(t, db, "k", "after") // seq 2, into segment 2
	wantGet(t, db, "k", []byte("after"))
}

// A snapshot that fails after rotating leaves an empty current segment. The
// retry must reuse it rather than create it again (1.37), and must accept a
// snapshot file the failed attempt already renamed into place.
func TestSnapshotRetryAfterFailure(t *testing.T) {
	tests := []struct {
		name         string
		after, match func(op, name string) bool
	}{
		{"snapshot file sync fails", nil, func(op, name string) bool {
			return op == "sync" && strings.HasSuffix(name, ".snap.tmp")
		}},
		{"dir sync after the snapshot rename fails", renameOf(".snap.tmp"), isOp("syncdir")},
		{"cleanup fails", renameOf(".snap.tmp"), isOp("remove")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := newFS()
			db := mustOpen(t, fsys)
			mustPut(t, db, "k", "v1") // seq 1
			failOnce(fsys, tt.after, tt.match)
			if err := db.Snapshot(context.Background()); err == nil || errors.Is(err, ErrFailed) {
				t.Fatalf("Snapshot with injected error: err = %v, want a plain error", err)
			}
			fsys.SetHook(nil)
			mustPut(t, db, "k", "v2") // a failed snapshot doesn't stop writes: seq 2

			mustSnapshot(t, db)
			wantFiles(t, fsys, snap(2), seg(3))
			mustClose(t, db)
			fsys.Crash(crashfs.DropUnsynced())
			db = mustOpen(t, fsys)
			defer mustClose(t, db)
			wantGet(t, db, "k", []byte("v2"))
		})
	}
}

// The retry with no write in between: the earlier attempt may have renamed
// snap-1 into place already.
func TestSnapshotRetryWithNothingWrittenInBetween(t *testing.T) {
	for _, failRename := range []bool{false, true} {
		fsys := newFS()
		db := mustOpen(t, fsys)
		mustPut(t, db, "k", "v1")
		if failRename {
			failOnce(fsys, renameOf(".snap.tmp"), isOp("syncdir")) // snap-1 exists, name not durable
		} else {
			failOnce(fsys, nil, func(op, name string) bool { return op == "sync" && strings.HasSuffix(name, ".snap.tmp") })
		}
		if err := db.Snapshot(context.Background()); err == nil {
			t.Fatalf("failRename=%v: Snapshot with injected error succeeded", failRename)
		}
		fsys.SetHook(nil)
		mustSnapshot(t, db)
		wantFiles(t, fsys, snap(1), seg(2))
		mustClose(t, db)
		fsys.Crash(crashfs.DropUnsynced())
		db = mustOpen(t, fsys)
		wantFiles(t, fsys, snap(1), seg(2))
		wantGet(t, db, "k", []byte("v1"))
		mustClose(t, db)
	}
}

// The DB-level failed flag matters once rotation can replace a failed writer
// with a fresh one.
func TestSnapshotAfterWALFailureIsRefused(t *testing.T) {
	fsys := newFS()
	db := mustOpen(t, fsys)
	defer mustClose(t, db)
	mustPut(t, db, "k", "v")
	failOnce(fsys, nil, isOp("sync"))
	if err := db.Put(context.Background(), []byte("lost"), nil); !errors.Is(err, ErrFailed) {
		t.Fatalf("Put: %v", err)
	}
	fsys.SetHook(nil)
	if err := db.Snapshot(context.Background()); !errors.Is(err, ErrFailed) {
		t.Fatalf("Snapshot after WAL failure: err = %v, want ErrFailed", err)
	}
	if err := db.Put(context.Background(), []byte("k"), nil); !errors.Is(err, ErrFailed) {
		t.Fatalf("Put after refused Snapshot: err = %v, want ErrFailed", err)
	}
	wantFiles(t, fsys, seg(1))
}

// CreateSegment failing after its rename leaves the new segment's name in
// place, so the DB must stop rather than keep appending to the old one.
func TestRotationFailureFailsDB(t *testing.T) {
	fsys := newFS()
	db := mustOpen(t, fsys)
	mustPut(t, db, "k", "v") // seq 1
	failOnce(fsys, renameOf(".log.tmp"), isOp("syncdir"))
	if err := db.Snapshot(context.Background()); !errors.Is(err, ErrFailed) {
		t.Fatalf("Snapshot whose rotation fails: err = %v, want ErrFailed", err)
	}
	fsys.SetHook(nil)
	if err := db.Put(context.Background(), []byte("k"), []byte("v2")); !errors.Is(err, ErrFailed) {
		t.Fatalf("Put after failed rotation: err = %v, want ErrFailed", err)
	}
	wantGet(t, db, "k", []byte("v"))
	mustClose(t, db)

	// Whether or not the new segment's name survives, recovery works and
	// writes resume.
	for _, crash := range []bool{false, true} {
		if crash {
			fsys.Crash(crashfs.DropUnsynced())
		}
		db = mustOpen(t, fsys)
		wantGet(t, db, "k", []byte("v"))
		mustClose(t, db)
	}
}

// Crash before each filesystem op of a Snapshot in turn (rotation, file
// write, install, cleanup), under both policies. Recovery must always
// succeed with every acknowledged write, and the files left must converge to
// one snapshot plus the current segment once a Snapshot completes.
func TestCrashDuringSnapshotAtEveryOp(t *testing.T) {
	model := map[string]string{}
	setup := func(t *testing.T) (*crashfs.FS, *DB) {
		fsys := newFS()
		db := mustOpen(t, fsys)
		for i := range 10 { // seqs 1..10
			mustPut(t, db, fmt.Sprintf("k%d", i), "old")
			model[fmt.Sprintf("k%d", i)] = "old"
		}
		mustSnapshot(t, db) // snap-10, wal-11: gives cleanup something to delete next time
		for i := range 5 {  // seqs 11..15
			mustPut(t, db, fmt.Sprintf("k%d", i), "new")
			model[fmt.Sprintf("k%d", i)] = "new"
		}
		if err := db.Delete(context.Background(), []byte("k9")); err != nil { // seq 16
			t.Fatal(err)
		}
		delete(model, "k9")
		return fsys, db
	}

	for k := 1; ; k++ {
		completed := false
		for _, policy := range []crashfs.Policy{crashfs.DropUnsynced(), crashfs.Random(rand.New(rand.NewPCG(uint64(k), 0)))} {
			fsys, db := setup(t)
			ops := 0
			fsys.SetHook(func(string, string) error {
				if ops++; ops == k {
					fsys.Crash(policy)
					return crashfs.ErrCrashed
				}
				return nil
			})
			err := db.Snapshot(context.Background())
			fsys.SetHook(nil)
			if completed = ops < k; completed {
				if err != nil {
					t.Fatalf("op %d: Snapshot without a crash: %v", k, err)
				}
				fsys.Crash(policy)
			}

			db = mustOpen(t, fsys)
			for i := range 10 {
				key := fmt.Sprintf("k%d", i)
				if want, ok := model[key]; ok {
					wantGet(t, db, key, []byte(want))
				} else {
					wantGet(t, db, key, nil)
				}
			}
			mustSnapshot(t, db) // finishes whatever the crash interrupted
			wantFiles(t, fsys, snap(16), seg(17))
			mustClose(t, db)
		}
		if completed {
			t.Logf("crashed before each of the first %d ops", k-1)
			return
		}
	}
}

// Under -race: snapshots while writers run. Every acknowledged write survives
// a crash right after.
func TestSnapshotConcurrentWithWrites(t *testing.T) {
	fsys := newFS()
	db := mustOpen(t, fsys)
	const writers, perWriter = 4, 100
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				key := fmt.Sprintf("w%d-%d", w, i)
				if err := db.Put(context.Background(), []byte(key), []byte(key)); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		})
	}
	for range 5 {
		mustSnapshot(t, db)
	}
	wg.Wait()
	mustSnapshot(t, db)

	fsys.Crash(crashfs.DropUnsynced())
	db = mustOpen(t, fsys)
	defer mustClose(t, db)
	for w := range writers {
		for i := range perWriter {
			key := fmt.Sprintf("w%d-%d", w, i)
			wantGet(t, db, key, []byte(key))
		}
	}
	wantFiles(t, fsys, snap(writers*perWriter), seg(writers*perWriter+1))
}
