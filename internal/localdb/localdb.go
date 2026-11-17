// Package localdb is the Phase 1 single-node database: a sorted in-memory map
// made durable by a WAL, recovered on Open. It is deleted in Phase 3, when the
// Raft log takes over the WAL's job and the store becomes the Raft FSM.
package localdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/dev64-kartik/kvstore/internal/store"
	"github.com/dev64-kartik/kvstore/internal/vfs"
	"github.com/dev64-kartik/kvstore/internal/wal"
)

var (
	// ErrNotFound: Get found no value for the key.
	ErrNotFound = errors.New("localdb: key not found")
	// ErrInvalidArgument: a key or value is outside the size limits.
	ErrInvalidArgument = errors.New("localdb: invalid argument")
	// ErrFailed: an earlier WAL write or sync failed, so the DB accepts no more
	// writes. Reads keep working. Reopen to recover (design doc: "Write
	// failure policy").
	ErrFailed = errors.New("localdb: failed after a WAL error; reopen to recover")
	// ErrClosed: the DB was closed.
	ErrClosed = errors.New("localdb: closed")
)

// Size limits. Keys must be non-empty.
const (
	MaxKeySize   = wal.MaxKeySize
	MaxValueSize = wal.MaxValueSize
)

// Options configures Open. The zero value uses the real filesystem and
// slog.Default().
type Options struct {
	FS     vfs.FS // crash tests pass a crashfs.FS
	Logger *slog.Logger
}

// DB is safe for concurrent use.
//
// Lock order: writeSlot before mu, never the reverse. Readers take only mu.
type DB struct {
	log *slog.Logger

	// writeSlot serializes the write path. It is a 1-slot channel rather than
	// a sync.Mutex because acquiring it must be abandonable when ctx is
	// cancelled, and Mutex.Lock can't be. Full means held.
	writeSlot chan struct{}
	// Guarded by writeSlot:
	wal    *wal.Writer
	failed error // the WAL error that failed the DB; set once, never cleared

	mu     sync.RWMutex
	tree   *store.Map // guarded by mu; holds only acknowledged writes
	closed bool       // guarded by writeSlot and mu both: written holding both, read holding either
}

// Open recovers the DB in dir, which must exist: load the newest snapshot,
// then replay the WAL records after it. A torn tail on the last segment is a
// normal crash artifact and is cut off (and logged). Anything else wrong is
// an error wrapping store.ErrCorrupt or wal.ErrCorrupt, and nothing is
// repaired or skipped.
func Open(dir string, opts Options) (*DB, error) {
	if opts.FS == nil {
		opts.FS = vfs.OS()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	tree, snapSeq, err := store.LoadLatest(opts.FS, dir)
	if err != nil {
		return nil, fmt.Errorf("localdb: open %s: %w", dir, err)
	}
	// Replay applies records straight to the tree. Map.Put copies, which
	// matters: a Record points into wal.Open's read buffer.
	replayed := 0
	w, info, err := wal.Open(opts.FS, dir, snapSeq, func(r wal.Record) error {
		switch r.Op {
		case wal.OpPut:
			tree.Put(r.Key, r.Value)
		case wal.OpDelete:
			tree.Delete(r.Key)
		default:
			// The decoder already rejects unknown ops; this guards against the
			// two drifting apart.
			return fmt.Errorf("%w: seq %d: unknown op %d", wal.ErrCorrupt, r.Seq, r.Op)
		}
		replayed++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("localdb: open %s: %w", dir, err)
	}

	if info.TornBytes > 0 {
		opts.Logger.Warn("localdb: cut torn WAL tail left by a crash",
			"segment", info.TornSegment, "bytes", info.TornBytes)
	}
	opts.Logger.Info("localdb: opened", "dir", dir, "snapshot_seq", snapSeq,
		"replayed", replayed, "keys", tree.Len(), "next_seq", w.NextSeq())

	return &DB{
		log:       opts.Logger,
		writeSlot: make(chan struct{}, 1),
		wal:       w,
		tree:      tree,
	}, nil
}

// Get returns a copy of the value for key, or ErrNotFound. It does no I/O
// and never waits for a write's fsync, only for the in-memory update.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	v, ok := db.tree.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

// Put durably sets key to value. When it returns nil, the write is on disk
// and visible to every later Get.
//
// ctx is honored only while waiting for earlier writes. Once this write's WAL
// record has started, it runs to completion: abandoning it would leave us
// unsure what's on disk.
func (db *DB) Put(ctx context.Context, key, value []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if len(value) > MaxValueSize {
		return fmt.Errorf("%w: value is %d bytes, max %d", ErrInvalidArgument, len(value), MaxValueSize)
	}
	return db.write(ctx, wal.OpPut, key, value)
}

// Delete durably removes key. Deleting a missing key succeeds, and is still
// logged, which keeps the write path uniform.
func (db *DB) Delete(ctx context.Context, key []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	return db.write(ctx, wal.OpDelete, key, nil)
}

func (db *DB) write(ctx context.Context, op wal.Op, key, value []byte) error {
	if err := db.acquire(ctx); err != nil {
		return err
	}
	defer db.release()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return fmt.Errorf("%w: %w", ErrFailed, db.failed)
	}

	// Durable first, visible second. In the other order a Get could return a
	// value that a crash then erases: a read of a write that never happened.
	if _, err := db.wal.Append(op, key, value); err != nil {
		if errors.Is(err, wal.ErrFailed) {
			db.failed = err
			db.log.Error("localdb: WAL failed; rejecting all further writes", "err", err)
			return fmt.Errorf("%w: %w", ErrFailed, err)
		}
		return err // can't happen after our own validation, but don't hide it
	}

	// The linearization point: from here every Get sees this write.
	db.mu.Lock()
	if op == wal.OpPut {
		db.tree.Put(key, value)
	} else {
		db.tree.Delete(key)
	}
	db.mu.Unlock()
	return nil
}

// acquire takes the writer slot, giving up if ctx ends first.
func (db *DB) acquire(ctx context.Context) error {
	select {
	case db.writeSlot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	// If both cases were ready, select picked one at random. Checking again
	// makes "ctx already cancelled" mean "nothing written", always.
	if err := ctx.Err(); err != nil {
		db.release()
		return err
	}
	return nil
}

func (db *DB) release() { <-db.writeSlot }

// Close waits for the write in progress, if any, then closes the WAL. Every
// later call, including Close, returns ErrClosed.
//
// Close doesn't sync: every acknowledged write already was.
func (db *DB) Close() error {
	db.writeSlot <- struct{}{} // no ctx: Close must not leave a write half-done
	defer db.release()
	if db.closed {
		return ErrClosed
	}
	db.mu.Lock()
	db.closed = true
	db.mu.Unlock()
	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("localdb: close: %w", err)
	}
	return nil
}

func checkKey(key []byte) error {
	if len(key) == 0 || len(key) > MaxKeySize {
		return fmt.Errorf("%w: key is %d bytes, want 1..%d", ErrInvalidArgument, len(key), MaxKeySize)
	}
	return nil
}
