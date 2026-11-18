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
	// ErrNotFound and ErrInvalidArgument are the data model's errors, so
	// errors.Is matches them against either name.
	ErrNotFound        = store.ErrNotFound
	ErrInvalidArgument = store.ErrInvalidArgument
	// ErrFailed: an earlier WAL write or sync failed, so the DB accepts no more
	// writes. Reads keep working. Reopen to recover (design doc: "Write
	// failure policy").
	ErrFailed = errors.New("localdb: failed after a WAL error; reopen to recover")
	// ErrClosed: the DB was closed.
	ErrClosed = errors.New("localdb: closed")
)

// Size limits, from the data model. Keys must be non-empty.
const (
	MaxKeySize   = store.MaxKeySize
	MaxValueSize = store.MaxValueSize
)

// Options configures Open. The zero value uses the real filesystem and
// slog.Default().
type Options struct {
	FS     vfs.FS // crash tests pass a crashfs.FS
	Logger *slog.Logger
}

// DB is safe for concurrent use.
//
// Lock order: snapSlot, then writeSlot, then mu; never the reverse. Readers
// take only mu, writers only writeSlot and mu.
type DB struct {
	dir  string
	fsys vfs.FS
	log  *slog.Logger

	// snapSlot serializes Snapshot (and Close against it). Writing the file
	// happens outside writeSlot, so writes continue meanwhile.
	snapSlot chan struct{}
	snapSeq  uint64 // seq of the newest installed snapshot; guarded by snapSlot

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
		dir:       dir,
		fsys:      opts.FS,
		log:       opts.Logger,
		snapSlot:  make(chan struct{}, 1),
		snapSeq:   snapSeq,
		writeSlot: make(chan struct{}, 1),
		wal:       w,
		tree:      tree,
	}, nil
}

// Get returns a copy of the value for key, or ErrNotFound. It does no I/O
// and never waits for a write's fsync, only for the in-memory update.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := store.CheckKey(key); err != nil {
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
	if err := store.CheckKey(key); err != nil {
		return err
	}
	if err := store.CheckValue(value); err != nil {
		return err
	}
	return db.write(ctx, wal.OpPut, key, value)
}

// Delete durably removes key. Deleting a missing key succeeds, and is still
// logged, which keeps the write path uniform.
func (db *DB) Delete(ctx context.Context, key []byte) error {
	if err := store.CheckKey(key); err != nil {
		return err
	}
	return db.write(ctx, wal.OpDelete, key, nil)
}

func (db *DB) write(ctx context.Context, op wal.Op, key, value []byte) error {
	if err := acquire(ctx, db.writeSlot); err != nil {
		return err
	}
	defer release(db.writeSlot)
	if err := db.writable(); err != nil {
		return err
	}

	// Durable first, visible second. In the other order a Get could return a
	// value that a crash then erases: a read of a write that never happened.
	if _, err := db.wal.Append(op, key, value); err != nil {
		if errors.Is(err, wal.ErrFailed) {
			return db.fail(err)
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

// writable reports why writes are refused, if they are. The caller holds
// writeSlot.
func (db *DB) writable() error {
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return fmt.Errorf("%w: %w", ErrFailed, db.failed)
	}
	return nil
}

// fail puts the DB in the failed state for good. The caller holds writeSlot.
func (db *DB) fail(err error) error {
	db.failed = err
	db.log.Error("localdb: WAL failed; rejecting all further writes", "err", err)
	return fmt.Errorf("%w: %w", ErrFailed, err)
}

// acquire takes a slot (writeSlot or snapSlot), giving up if ctx ends first.
func acquire(ctx context.Context, slot chan struct{}) error {
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	// If both cases were ready, select picked one at random. Checking again
	// makes "ctx already cancelled" mean "nothing done", always.
	if err := ctx.Err(); err != nil {
		release(slot)
		return err
	}
	return nil
}

func release(slot chan struct{}) { <-slot }

// Close waits for the snapshot and write in progress, if any, then closes the
// WAL. Every later call, including Close, returns ErrClosed.
//
// Close doesn't sync: every acknowledged write already was.
func (db *DB) Close() error {
	// No ctx: Close must not leave a write or snapshot half-done.
	db.snapSlot <- struct{}{}
	defer release(db.snapSlot)
	db.writeSlot <- struct{}{}
	defer release(db.writeSlot)
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
