package wal

import (
	"fmt"
	"path/filepath"
	"slices"

	"github.com/dev64-kartik/kvstore/internal/vfs"
)

// RemoveCovered deletes every segment whose records all have seq <= snapSeq,
// and returns how many it deleted. Call it only after the snapshot covering
// snapSeq is durably installed: until then, recovery may need these segments.
//
// A segment's last seq isn't in its name, but it is one less than the next
// segment's start. So a segment is covered when the next one starts at or
// before snapSeq+1. The last segment has no successor and is never deleted.
//
// The removals aren't dir-synced here; the caller syncs the directory once
// for all its cleanup. If a crash undoes some of them, Open skips covered
// segments without reading them, and the next call deletes them again.
func RemoveCovered(fsys vfs.FS, dir string, snapSeq uint64) (int, error) {
	names, err := fsys.List(dir)
	if err != nil {
		return 0, fmt.Errorf("wal: gc: %w", err)
	}
	var starts []uint64
	for _, name := range names {
		if s, ok := parseSegmentName(name); ok {
			starts = append(starts, s)
		}
	}
	slices.Sort(starts)

	removed := 0
	for i := 0; i+1 < len(starts); i++ {
		if starts[i+1] > snapSeq+1 {
			break // this segment holds seq snapSeq+1 or later; so do all after it
		}
		if err := fsys.Remove(filepath.Join(dir, segmentName(starts[i]))); err != nil {
			return removed, fmt.Errorf("wal: gc: %w", err)
		}
		removed++
	}
	return removed, nil
}
