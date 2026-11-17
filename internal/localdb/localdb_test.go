package localdb

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/store"
	"github.com/dev64-kartik/kvstore/internal/vfs/crashfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

const testDir = "/db"

var seedFlag = flag.Uint64("seed", 0, "seed for randomized tests (0 = pick one)")

func testSeed(t *testing.T) uint64 {
	t.Helper()
	seed := *seedFlag
	if seed == 0 {
		seed = rand.Uint64()
	}
	t.Logf("seed %d (replay with -seed=%d)", seed, seed)
	return seed
}

func newFS() *crashfs.FS {
	fsys := crashfs.New()
	fsys.Mkdir(testDir)
	return fsys
}

func mustOpen(t *testing.T, fsys *crashfs.FS) *DB {
	t.Helper()
	db, err := Open(testDir, Options{FS: fsys, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func mustClose(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func mustPut(t *testing.T, db *DB, key, value string) {
	t.Helper()
	if err := db.Put(context.Background(), []byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

// wantGet checks Get(key). want nil means the key must be missing.
func wantGet(t *testing.T, db *DB, key string, want []byte) {
	t.Helper()
	got, err := db.Get([]byte(key))
	switch {
	case want == nil && !errors.Is(err, ErrNotFound):
		t.Fatalf("Get(%q) = %q, %v; want ErrNotFound", key, got, err)
	case want != nil && (err != nil || !bytes.Equal(got, want)):
		t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, want)
	}
}

func TestPutGetDelete(t *testing.T) {
	db := mustOpen(t, newFS())
	defer mustClose(t, db)
	ctx := context.Background()

	wantGet(t, db, "k", nil)
	mustPut(t, db, "k", "v1")
	mustPut(t, db, "k", "v2")
	wantGet(t, db, "k", []byte("v2"))
	mustPut(t, db, "empty", "")
	wantGet(t, db, "empty", []byte{}) // an empty value is present, not missing
	if err := db.Delete(ctx, []byte("k")); err != nil {
		t.Fatal(err)
	}
	wantGet(t, db, "k", nil)
	if err := db.Delete(ctx, []byte("never-existed")); err != nil {
		t.Fatalf("Delete of a missing key: %v", err)
	}
}

// Random ops against a plain map, with clean reopens and crashes mixed in.
// Every op here is acknowledged before the crash, so all of them must survive.
func TestMatchesModelAcrossReopensAndCrashes(t *testing.T) {
	rng := rand.New(rand.NewPCG(testSeed(t), 0))
	fsys := newFS()
	db := mustOpen(t, fsys)
	model := map[string][]byte{}
	ctx := context.Background()

	for i := range 2000 {
		key := fmt.Sprintf("k%d", rng.IntN(50))
		switch r := rng.IntN(100); {
		case r < 60:
			v := make([]byte, rng.IntN(64))
			for j := range v {
				v[j] = byte(rng.Uint32())
			}
			if err := db.Put(ctx, []byte(key), v); err != nil {
				t.Fatalf("op %d: Put: %v", i, err)
			}
			model[key] = v
		case r < 90:
			if err := db.Delete(ctx, []byte(key)); err != nil {
				t.Fatalf("op %d: Delete: %v", i, err)
			}
			delete(model, key)
		case r < 95:
			mustClose(t, db)
			db = mustOpen(t, fsys)
		default:
			// No Close: the crash kills the WAL handle, as power loss would.
			fsys.Crash(crashfs.Random(rng))
			db = mustOpen(t, fsys)
		}
	}
	for k := range 50 {
		key := fmt.Sprintf("k%d", k)
		wantGet(t, db, key, model[key])
	}
	mustClose(t, db)
}

func TestInvalidArgumentsWriteNothing(t *testing.T) {
	db := mustOpen(t, newFS())
	defer mustClose(t, db)
	ctx := context.Background()
	big := make([]byte, MaxKeySize+1)
	tests := []struct {
		name string
		call func() error
	}{
		{"Put empty key", func() error { return db.Put(ctx, nil, []byte("v")) }},
		{"Put key too long", func() error { return db.Put(ctx, big, []byte("v")) }},
		{"Put value too long", func() error { return db.Put(ctx, []byte("k"), make([]byte, MaxValueSize+1)) }},
		{"Delete empty key", func() error { return db.Delete(ctx, []byte{}) }},
		{"Get key too long", func() error { _, err := db.Get(big); return err }},
	}
	for _, tt := range tests {
		if err := tt.call(); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: err = %v, want ErrInvalidArgument", tt.name, err)
		}
	}
	if next := db.wal.NextSeq(); next != 1 {
		t.Fatalf("NextSeq = %d after only invalid ops, want 1", next)
	}
	// The limits themselves are allowed.
	if err := db.Put(ctx, make([]byte, MaxKeySize), make([]byte, MaxValueSize)); err != nil {
		t.Fatalf("Put at the limits: %v", err)
	}
}

func TestCallerMayReuseBuffers(t *testing.T) {
	db := mustOpen(t, newFS())
	defer mustClose(t, db)
	key, value := []byte("k"), []byte("v1")
	if err := db.Put(context.Background(), key, value); err != nil {
		t.Fatal(err)
	}
	key[0], value[1] = 'x', '9' // reuse the caller's buffers
	wantGet(t, db, "k", []byte("v1"))
	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	got[0] = 'X' // and scribble on the returned copy
	wantGet(t, db, "k", []byte("v1"))
}

func TestCancelledContextWritesNothing(t *testing.T) {
	db := mustOpen(t, newFS())
	defer mustClose(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Slot free: select may pick either ready case, so repeat to cover both.
	for range 100 {
		if err := db.Put(ctx, []byte("k"), []byte("v")); !errors.Is(err, context.Canceled) {
			t.Fatalf("Put with cancelled ctx: err = %v", err)
		}
	}
	// Slot held by another writer: the wait itself must give up. Without the
	// ctx.Done case, this Put would block forever.
	db.writeSlot <- struct{}{}
	err := db.Delete(ctx, []byte("k"))
	db.release()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete waiting for a held slot: err = %v", err)
	}

	if next := db.wal.NextSeq(); next != 1 {
		t.Fatalf("NextSeq = %d, want 1: a cancelled write reached the WAL", next)
	}
	wantGet(t, db, "k", nil)
}

func TestWALFailureFailsWritesButNotReads(t *testing.T) {
	fsys := newFS()
	db := mustOpen(t, fsys)
	ctx := context.Background()
	mustPut(t, db, "before", "ok")

	failNext := true
	fsys.SetHook(func(op, _ string) error {
		if op == "sync" && failNext {
			failNext = false
			return errors.New("injected EIO")
		}
		return nil
	})
	if err := db.Put(ctx, []byte("lost"), []byte("x")); !errors.Is(err, ErrFailed) {
		t.Fatalf("Put whose fsync fails: err = %v, want ErrFailed", err)
	}
	// The disk is "healthy" again, but the DB must not trust it (fsyncgate).
	if err := db.Put(ctx, []byte("after"), []byte("x")); !errors.Is(err, ErrFailed) {
		t.Fatalf("Put after failure: err = %v, want ErrFailed", err)
	}
	if err := db.Delete(ctx, []byte("before")); !errors.Is(err, ErrFailed) {
		t.Fatalf("Delete after failure: err = %v, want ErrFailed", err)
	}
	wantGet(t, db, "before", []byte("ok"))
	wantGet(t, db, "lost", nil) // never acknowledged, so never visible
	mustClose(t, db)

	db = mustOpen(t, fsys)
	defer mustClose(t, db)
	wantGet(t, db, "before", []byte("ok"))
	wantGet(t, db, "after", nil)
	// "lost" may or may not be back: its bytes reached the (simulated) page
	// cache. That is edge case 1.11; both outcomes are allowed.
	mustPut(t, db, "after", "reopened") // reopening recovers the write path
}

// Under -race: writers and readers in parallel. After each acknowledged Put,
// the writer's own Get must see it (a Get that starts after a Put returns
// always sees it).
func TestConcurrentWritersAndReaders(t *testing.T) {
	db := mustOpen(t, newFS())
	defer mustClose(t, db)
	const writers, perWriter = 8, 50
	done := make(chan struct{})
	var readers, ws sync.WaitGroup

	for r := range 4 {
		readers.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				key := fmt.Sprintf("w%d-%d", i%writers, (i+r)%perWriter)
				if v, err := db.Get([]byte(key)); err == nil && !bytes.HasPrefix(v, []byte(key)) {
					t.Errorf("Get(%q) = %q: someone else's value", key, v)
					return
				} else if err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("Get(%q): %v", key, err)
					return
				}
			}
		})
	}
	for w := range writers {
		ws.Go(func() {
			for i := range perWriter {
				key := fmt.Sprintf("w%d-%d", w, i)
				if err := db.Put(context.Background(), []byte(key), []byte(key+"=v")); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if v, err := db.Get([]byte(key)); err != nil || string(v) != key+"=v" {
					t.Errorf("Get(%q) right after its Put = %q, %v", key, v, err)
					return
				}
			}
		})
	}
	ws.Wait()
	close(done)
	readers.Wait()
	if next := db.wal.NextSeq(); next != writers*perWriter+1 {
		t.Fatalf("NextSeq = %d, want %d", next, writers*perWriter+1)
	}
}

func TestClosedDBRejectsEverything(t *testing.T) {
	db := mustOpen(t, newFS())
	mustPut(t, db, "k", "v")
	mustClose(t, db)
	ctx := context.Background()
	if err := db.Put(ctx, []byte("k"), nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Put: %v", err)
	}
	if err := db.Delete(ctx, []byte("k")); !errors.Is(err, ErrClosed) {
		t.Errorf("Delete: %v", err)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Errorf("Get: %v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second Close: %v", err)
	}
}

func TestOpenRefusesCorruptSnapshot(t *testing.T) {
	fsys := newFS()
	vfstest.WriteFile(t, fsys, filepath.Join(testDir, "snap-00000000000000000003.snap"), "garbage")
	if err := fsys.SyncDir(testDir); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(testDir, Options{FS: fsys}); !errors.Is(err, store.ErrCorrupt) {
		t.Fatalf("Open: err = %v, want store.ErrCorrupt", err)
	}
}
