package wal

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/dev64-kartik/kvstore/internal/vfs"
)

// OpenInfo reports what recovery did, for the caller to log.
type OpenInfo struct {
	TornBytes   int    // bytes cut from the end of the last segment; 0 if it ended cleanly
	TornSegment string // path of the segment that was cut, if any
}

// Open recovers the WAL in dir and returns a Writer that appends after the
// last good record.
//
// afterSeq is the last seq already covered by a snapshot (0 if none). Records
// with seq <= afterSeq are validated but not passed to apply. Segments that
// lie wholly within the snapshot aren't read at all, since GC (step 7) may not
// have deleted them yet. A Record passed to apply is valid only during the
// call.
//
// Recovery:
//  1. Remove leftover segment temp files.
//  2. With no segments: start a fresh log at seq 1, or fail if afterSeq > 0
//     (the snapshot would be followed by a missing log).
//  3. Replay each sealed segment in order. Each must start exactly where the
//     previous one ended, and any bad record in them is ErrCorrupt.
//  4. Replay the last segment, cut off a torn tail if there is one, and keep
//     its handle open for appending.
//  5. The log must reach at least afterSeq.
func Open(fsys vfs.FS, dir string, afterSeq uint64, apply func(Record) error) (_ *Writer, _ OpenInfo, err error) {
	names, err := fsys.List(dir)
	if err != nil {
		return nil, OpenInfo{}, fmt.Errorf("wal: open: %w", err)
	}
	if err := removeTempSegments(fsys, dir, names); err != nil {
		return nil, OpenInfo{}, err
	}
	var starts []uint64
	for _, name := range names {
		if s, ok := parseSegmentName(name); ok {
			starts = append(starts, s)
		}
	}
	slices.Sort(starts)

	if len(starts) == 0 {
		if afterSeq != 0 {
			return nil, OpenInfo{}, fmt.Errorf("%w: no segments, but the snapshot covers through seq %d", ErrCorrupt, afterSeq)
		}
		w, err := CreateSegment(fsys, dir, 1)
		return w, OpenInfo{}, err
	}

	// Replay starts at the newest segment that begins at or before
	// afterSeq+1. Earlier segments are entirely inside the snapshot.
	first := -1
	for i, s := range starts {
		if s <= afterSeq+1 {
			first = i
		}
	}
	if first < 0 {
		return nil, OpenInfo{}, fmt.Errorf("%w: seqs %d..%d missing: the snapshot covers through %d, the oldest segment starts at %d",
			ErrCorrupt, afterSeq+1, starts[0]-1, afterSeq, starts[0])
	}

	replay := func(r Record) error {
		if r.Seq <= afterSeq {
			return nil
		}
		return apply(r)
	}
	next := starts[first] // the startSeq the next segment must have
	for _, start := range starts[first : len(starts)-1] {
		if start != next {
			return nil, OpenInfo{}, segmentGap(next, start)
		}
		lastSeq, err := replaySealed(fsys, filepath.Join(dir, segmentName(start)), start, replay)
		if err != nil {
			return nil, OpenInfo{}, err
		}
		next = lastSeq + 1
	}
	lastStart := starts[len(starts)-1]
	if lastStart != next {
		return nil, OpenInfo{}, segmentGap(next, lastStart)
	}

	w, info, err := openLast(fsys, filepath.Join(dir, segmentName(lastStart)), lastStart, replay)
	if err != nil {
		return nil, OpenInfo{}, err
	}
	if w.nextSeq-1 < afterSeq {
		// Appending here would reuse seqs the snapshot already covers.
		return nil, OpenInfo{}, errors.Join(
			fmt.Errorf("%w: log ends at seq %d, but the snapshot covers through %d", ErrCorrupt, w.nextSeq-1, afterSeq),
			w.Close())
	}
	return w, info, nil
}

func segmentGap(want, got uint64) error {
	return fmt.Errorf("%w: missing segment: expected one starting at seq %d, next found starts at %d", ErrCorrupt, want, got)
}

// replaySealed reads and replays a segment that is no longer written to.
func replaySealed(fsys vfs.FS, path string, startSeq uint64, replay func(Record) error) (uint64, error) {
	data, err := readFile(fsys, path)
	if err != nil {
		return 0, err
	}
	info, err := scanSegment(path, data, startSeq, false, replay)
	return info.lastSeq, err
}

// openLast replays the last segment, cuts off a torn tail if there is one,
// and returns a Writer appending to it.
func openLast(fsys vfs.FS, path string, startSeq uint64, replay func(Record) error) (_ *Writer, _ OpenInfo, err error) {
	f, err := fsys.OpenAppend(path)
	if err != nil {
		return nil, OpenInfo{}, fmt.Errorf("wal: open: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, OpenInfo{}, fmt.Errorf("wal: read %s: %w", path, err)
	}
	info, err := scanSegment(path, data, startSeq, true, replay)
	if err != nil {
		return nil, OpenInfo{}, err
	}

	var oi OpenInfo
	if info.tornLen > 0 {
		// The tail must be cut before anything is appended. Otherwise the next
		// record would land after garbage, and the next recovery would
		// (rightly) call that corruption.
		if err := f.Truncate(int64(info.validLen)); err != nil {
			return nil, OpenInfo{}, fmt.Errorf("wal: truncate torn tail of %s: %w", path, err)
		}
		// Strictly, the first Append's Sync would make the cut durable too,
		// since fsync persists the file's size along with its data. Syncing
		// here means Open's result doesn't depend on that: when Open returns,
		// the file on disk ends at a record boundary.
		if err := f.Sync(); err != nil {
			return nil, OpenInfo{}, fmt.Errorf("wal: sync after truncating %s: %w", path, err)
		}
		oi = OpenInfo{TornBytes: info.tornLen, TornSegment: path}
	}
	// The handle was opened in append mode, so writes go to the (new) end.
	return &Writer{f: f, path: path, nextSeq: info.lastSeq + 1}, oi, nil
}

// removeTempSegments deletes leftover segment temp files. They come from a
// crash inside CreateSegment and hold at most a header, never an
// acknowledged record. They must go: CreateSegment uses O_EXCL, so a leftover
// would block that segment from ever being created.
//
// No SyncDir afterwards: if a crash undoes a removal, the next Open simply
// removes the file again. Cleanup that is safe to repeat doesn't need to be
// durable.
func removeTempSegments(fsys vfs.FS, dir string, names []string) error {
	for _, name := range names {
		base, isTemp := strings.CutSuffix(name, ".tmp")
		if _, isSegment := parseSegmentName(base); !isTemp || !isSegment {
			continue
		}
		if err := fsys.Remove(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("wal: remove leftover %s: %w", name, err)
		}
	}
	return nil
}

// parseSegmentName returns the startSeq in a segment file name. It accepts
// only names exactly as segmentName produces them, and ignores anything else
// in the directory (snapshots, temp files, stray files).
func parseSegmentName(name string) (uint64, bool) {
	digits, ok := strings.CutPrefix(name, "wal-")
	if !ok {
		return 0, false
	}
	digits, ok = strings.CutSuffix(digits, ".log")
	if !ok || len(digits) != 20 {
		return 0, false
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || seq == 0 {
		return 0, false
	}
	return seq, true
}

// readFile reads a whole file through fsys.
func readFile(fsys vfs.FS, path string) (_ []byte, err error) {
	f, err := fsys.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("wal: close %s: %w", path, cerr)
		}
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("wal: read %s: %w", path, err)
	}
	return data, nil
}
