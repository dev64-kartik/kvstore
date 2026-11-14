// Package store holds a node's in-memory key-value state: a sorted map whose
// O(1) copy-on-write Clone lets a snapshot be written without blocking writers.
package store

import (
	"bytes"

	"github.com/google/btree"
)

// btreeDegree is the B-tree's branching factor: every node except the root
// holds between degree-1 and 2*degree-1 entries. 32 keeps the tree shallow
// (about 4 levels for a million keys). A shallow tree means fewer node copies
// on the first write after a Clone, while inserting into one node's sorted
// slice stays cheap.
const btreeDegree = 32

type entry struct {
	key   []byte
	value []byte
}

// lessEntry orders entries by key, comparing bytes as unsigned values
// (0x00 < 'a' < 0xff, and "a" < "aa").
func lessEntry(a, b entry) bool {
	return bytes.Compare(a.key, b.key) < 0
}

// Map is a sorted map from byte-slice keys to byte-slice values.
//
// Concurrency: Map has no lock of its own. Callers (localdb now, the FSM in
// Phase 3) guard it with theirs. Concurrent reads are safe. Mutations must be
// exclusive with every other call on the same Map, including Clone. Once Clone
// returns, the original and the clone share no mutable state, so each can be
// used from a different goroutine.
//
// Ownership: Map copies keys and values on the way in and on the way out, so
// callers may reuse or modify their buffers freely.
type Map struct {
	tree *btree.BTreeG[entry]
}

// NewMap returns an empty Map.
func NewMap() *Map {
	return &Map{tree: btree.NewG(btreeDegree, lessEntry)}
}

// Get returns a copy of the value stored under key. A found value is never
// nil, even when empty, so "found but empty" can't be confused with "missing".
func (m *Map) Get(key []byte) ([]byte, bool) {
	e, ok := m.tree.Get(entry{key: key})
	if !ok {
		return nil, false
	}
	return copyBytes(e.value), true
}

// Put stores value under key, replacing any existing value.
func (m *Map) Put(key, value []byte) {
	// Always insert freshly allocated slices. Never write into an existing
	// entry's slices: after a Clone, entries are shared with the clone, and an
	// in-place write would silently change the snapshot's frozen view.
	// ReplaceOrInsert swaps in the new entry and leaves the old one untouched,
	// so any clone still referencing it is unaffected.
	m.tree.ReplaceOrInsert(entry{key: copyBytes(key), value: copyBytes(value)})
}

// Delete removes key. Deleting a missing key is a no-op.
func (m *Map) Delete(key []byte) {
	m.tree.Delete(entry{key: key})
}

// Len returns the number of keys.
func (m *Map) Len() int {
	return m.tree.Len()
}

// Clone returns a point-in-time copy of m in O(1). Nodes are shared and copied
// lazily: whichever map is written to next copies the nodes on the path it
// touches, so neither map ever sees the other's later writes.
//
// Clone must not run concurrently with any mutation of m: it replaces m's
// internal copy-on-write marker, which every mutation reads.
func (m *Map) Clone() *Map {
	return &Map{tree: m.tree.Clone()}
}

// Ascend calls fn for each entry in ascending key order, stopping early if fn
// returns false.
//
// The slices passed to fn are the map's own storage, not copies, which avoids
// copying the whole data set when a snapshot is written. fn must not modify
// them.
func (m *Map) Ascend(fn func(key, value []byte) bool) {
	m.tree.Ascend(func(e entry) bool {
		return fn(e.key, e.value)
	})
}

// copyBytes returns a fresh copy of b. Unlike bytes.Clone, it never returns
// nil: an empty value is stored and returned as a non-nil empty slice.
func copyBytes(b []byte) []byte {
	return append(make([]byte, 0, len(b)), b...)
}
