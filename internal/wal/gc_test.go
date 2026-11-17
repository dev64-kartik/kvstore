package wal

import (
	"slices"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

func TestRemoveCovered(t *testing.T) {
	// Segments hold 1..3, 4..5, 6..8 (the last).
	tests := []struct {
		snapSeq  uint64
		keepFrom []uint64
	}{
		{0, []uint64{1, 4, 6}},
		{2, []uint64{1, 4, 6}}, // 1..3 only partly covered
		{3, []uint64{4, 6}},    // boundary: 1..3 exactly covered
		{4, []uint64{4, 6}},
		{5, []uint64{6}},
		{8, []uint64{6}}, // the last segment always stays
		{100, []uint64{6}},
	}
	for _, tt := range tests {
		fsys := newCrashFS()
		writeSegment(t, fsys, 1, 3)
		writeSegment(t, fsys, 4, 2)
		writeSegment(t, fsys, 6, 3)
		removed, err := RemoveCovered(fsys, testDir, tt.snapSeq)
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for _, s := range tt.keepFrom {
			want = append(want, segmentName(s))
		}
		if got := vfstest.List(t, fsys, testDir); !slices.Equal(got, want) || removed != 3-len(want) {
			t.Fatalf("snapSeq %d: kept %q (removed %d), want %q", tt.snapSeq, got, removed, want)
		}
		// Whatever was removed, the log still recovers from the snapshot.
		w, _, _, err := openWAL(fsys, min(tt.snapSeq, 8))
		if err != nil {
			t.Fatalf("snapSeq %d: Open after gc: %v", tt.snapSeq, err)
		}
		closeWriter(t, w)
	}
}
