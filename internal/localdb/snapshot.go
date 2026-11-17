package localdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/dev64-kartik/kvstore/internal/store"
	"github.com/dev64-kartik/kvstore/internal/wal"
)

// Snapshot durably writes the current state to a snapshot file, then deletes
// the WAL segments and older snapshots it replaces. Writes continue while the
// file is written; only the rotation and the O(1) clone block them.
//
// If nothing was written since the last snapshot, no file is written, but
// cleanup still runs (it finishes the cleanup of a snapshot interrupted by a
// crash). A failed snapshot doesn't fail the DB and may simply be retried,
// except when rotating the WAL fails, which fails the DB (see rotate).
func (db *DB) Snapshot(ctx context.Context) error {
	if err := acquire(ctx, db.snapSlot); err != nil {
		return err
	}
	defer release(db.snapSlot)

	seq, clone, err := db.cut(ctx)
	if err != nil {
		return err
	}
	if clone != nil {
		err := store.WriteSnapshot(ctx, db.fsys, db.dir, seq, clone)
		if errors.Is(err, fs.ErrExist) {
			// An earlier attempt at this same seq got as far as the rename,
			// then failed (directory sync). That file is complete: it was
			// synced before the rename. And it holds exactly this state,
			// because the state at a seq is fixed by the log. Only its name
			// may not be durable yet.
			err = db.fsys.SyncDir(db.dir)
		}
		if err != nil {
			return fmt.Errorf("localdb: snapshot at seq %d: %w", seq, err)
		}
		db.snapSeq = seq
		db.log.Info("localdb: snapshot installed", "seq", seq, "keys", clone.Len())
	}
	return db.collectGarbage()
}

// cut fixes the state the snapshot will hold: everything through the last
// acknowledged seq. It returns a nil Map when that is already the newest
// snapshot. Holding writeSlot here is what makes the clone and seq agree: no
// write can land between reading NextSeq and cloning the tree.
func (db *DB) cut(ctx context.Context) (uint64, *store.Map, error) {
	if err := acquire(ctx, db.writeSlot); err != nil {
		return 0, nil, err
	}
	defer release(db.writeSlot)
	if err := db.writable(); err != nil {
		return 0, nil, err
	}

	lastSeq := db.wal.NextSeq() - 1
	if lastSeq == db.snapSeq {
		return lastSeq, nil, nil
	}
	// Start a new segment at lastSeq+1, so every older segment is wholly
	// covered by the snapshot and can be deleted once it's installed. If the
	// current segment is still empty, it already starts at lastSeq+1, and
	// creating another segment with that name would fail (edge case 1.37).
	// That happens when retrying a snapshot whose file write failed.
	if db.wal.StartSeq() <= lastSeq {
		if err := db.rotate(lastSeq + 1); err != nil {
			return 0, nil, err
		}
	}
	// google/btree forbids Clone concurrently with writes. Holding writeSlot
	// already rules those out; mu also orders it against readers for -race.
	db.mu.Lock()
	clone := db.tree.Clone()
	db.mu.Unlock()
	return lastSeq, clone, nil
}

// rotate switches the WAL to a new segment starting at startSeq. The caller
// holds writeSlot.
//
// Any failure here fails the DB. CreateSegment can fail after its rename, so
// the new segment's name may exist. If we then kept appending to the old
// segment, it would run past startSeq while a (header-only) segment named
// startSeq follows it, and the next Open would refuse to start (ErrCorrupt:
// segments don't line up). Fail-stop keeps every acknowledged write
// recoverable instead.
func (db *DB) rotate(startSeq uint64) error {
	w, err := wal.CreateSegment(db.fsys, db.dir, startSeq)
	if err != nil {
		return db.fail(fmt.Errorf("rotate WAL: %w", err))
	}
	old := db.wal
	db.wal = w
	// Every record in old was synced when appended, so nothing is lost if
	// Close fails. But a failed close is an I/O error we can't explain, and the
	// write failure policy is to stop, not guess.
	if err := old.Close(); err != nil {
		return db.fail(fmt.Errorf("close rotated WAL segment: %w", err))
	}
	return nil
}

// collectGarbage deletes what the newest snapshot makes unnecessary, then
// syncs the directory once. The caller holds snapSlot, which is what makes it
// safe to run without writeSlot: only Snapshot creates segments, and the
// segment being appended to is the newest, which RemoveCovered never deletes.
func (db *DB) collectGarbage() error {
	segments, err := wal.RemoveCovered(db.fsys, db.dir, db.snapSeq)
	if err != nil {
		return fmt.Errorf("localdb: snapshot cleanup: %w", err)
	}
	snapshots, err := store.RemoveOlder(db.fsys, db.dir, db.snapSeq)
	if err != nil {
		return fmt.Errorf("localdb: snapshot cleanup: %w", err)
	}
	if segments+snapshots == 0 {
		return nil
	}
	// Not needed for correctness (an undone removal is redone next time). It
	// makes "Snapshot returned nil" mean the disk space is really reclaimed.
	if err := db.fsys.SyncDir(db.dir); err != nil {
		return fmt.Errorf("localdb: snapshot cleanup: %w", err)
	}
	db.log.Info("localdb: removed files covered by snapshot", "seq", db.snapSeq,
		"segments", segments, "snapshots", snapshots)
	return nil
}
