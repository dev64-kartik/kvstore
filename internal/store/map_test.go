package store

import (
	"bytes"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// seedFlag lets a failing randomized run be replayed exactly:
//
//	go test -race -run TestMapMatchesModel ./internal/store -seed=<seed from the log>
var seedFlag = flag.Uint64("seed", 0, "seed for randomized tests (0 = derive from the clock)")

func testSeed(t *testing.T) uint64 {
	t.Helper()
	seed := *seedFlag
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("seed=%d (replay with -seed=%d)", seed, seed)
	return seed
}

// randKey draws from a deliberately small key space (85 keys: length 0..3 over
// a 4-symbol alphabet), so random ops keep hitting existing keys. The alphabet
// includes 0x00 and 0xff, and short keys are prefixes of longer ones. Those are
// the byte-ordering cases a sort bug would get wrong.
func randKey(rng *rand.Rand) []byte {
	alphabet := []byte{0x00, 0x01, 'a', 0xff}
	key := make([]byte, rng.IntN(4))
	for i := range key {
		key[i] = alphabet[rng.IntN(len(alphabet))]
	}
	return key
}

// randValue returns 0..8 random bytes. Length 0 covers empty values.
func randValue(rng *rand.Rand) []byte {
	v := make([]byte, rng.IntN(9))
	for i := range v {
		v[i] = byte(rng.Uint32())
	}
	return v
}

// TestMapMatchesModel runs random operations against Map and against a plain
// Go map (the model) and requires them to agree after every operation. It also
// takes clones along the way and checks, at the end, that each clone still
// matches the model as it was when the clone was taken.
func TestMapMatchesModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(testSeed(t), 0))
	m := NewMap()
	model := map[string][]byte{}

	type frozen struct {
		clone *Map
		want  map[string][]byte
	}
	var clones []frozen

	const ops = 20_000
	for i := range ops {
		key := randKey(rng)
		switch r := rng.IntN(100); {
		case r < 45:
			value := randValue(rng)
			m.Put(key, value)
			model[string(key)] = value
		case r < 70:
			m.Delete(key)
			delete(model, string(key))
		case r < 98:
			got, ok := m.Get(key)
			want, wantOK := model[string(key)]
			if ok != wantOK || !bytes.Equal(got, want) {
				t.Fatalf("op %d: Get(%q) = %q, %v; want %q, %v", i, key, got, ok, want, wantOK)
			}
			if ok && got == nil {
				t.Fatalf("op %d: Get(%q) returned nil for a present key", i, key)
			}
		default:
			// maps.Clone is shallow, which is fine: the test never mutates a
			// value slice after storing it in the model.
			clones = append(clones, frozen{clone: m.Clone(), want: maps.Clone(model)})
		}
	}

	checkContents(t, "final map", m, model)
	for i, c := range clones {
		checkContents(t, fmt.Sprintf("clone %d", i), c.clone, c.want)
	}
}

// checkContents verifies Len and that Ascend yields exactly the model's
// entries in sorted key order. Go compares strings bytewise, so sorting the
// model's string keys gives the same order as bytes.Compare.
func checkContents(t *testing.T, name string, m *Map, model map[string][]byte) {
	t.Helper()
	if m.Len() != len(model) {
		t.Fatalf("%s: Len() = %d, want %d", name, m.Len(), len(model))
	}
	wantKeys := slices.Sorted(maps.Keys(model))
	i := 0
	m.Ascend(func(key, value []byte) bool {
		if i >= len(wantKeys) {
			t.Fatalf("%s: Ascend yielded extra key %q", name, key)
		}
		if string(key) != wantKeys[i] {
			t.Fatalf("%s: Ascend position %d: key %q, want %q", name, i, key, wantKeys[i])
		}
		if !bytes.Equal(value, model[wantKeys[i]]) {
			t.Fatalf("%s: key %q: value %q, want %q", name, key, value, model[wantKeys[i]])
		}
		i++
		return true
	})
	if i != len(wantKeys) {
		t.Fatalf("%s: Ascend yielded %d keys, want %d", name, i, len(wantKeys))
	}
}

func TestMapCopiesInAndOut(t *testing.T) {
	m := NewMap()
	key := []byte("k")
	value := []byte("v1")
	m.Put(key, value)

	// The caller reuses its buffers after Put.
	key[0] = 'x'
	value[0] = 'X'
	got, ok := m.Get([]byte("k"))
	if !ok || string(got) != "v1" {
		t.Fatalf("after caller modified its buffers: Get(k) = %q, %v; want \"v1\", true", got, ok)
	}
	if _, ok := m.Get([]byte("x")); ok {
		t.Fatal("caller modifying its key buffer created key \"x\"")
	}

	// The caller writes into the slice Get returned.
	got[0] = 'Z'
	again, _ := m.Get([]byte("k"))
	if string(again) != "v1" {
		t.Fatalf("after caller modified Get's result: Get(k) = %q, want \"v1\"", again)
	}
}

func TestMapEmptyValueIsNotMissing(t *testing.T) {
	m := NewMap()
	m.Put([]byte("k"), nil)

	got, ok := m.Get([]byte("k"))
	if !ok || got == nil || len(got) != 0 {
		t.Fatalf("Get(k) = %#v, %v; want non-nil empty slice, true", got, ok)
	}
	if got, ok := m.Get([]byte("missing")); ok || got != nil {
		t.Fatalf("Get(missing) = %#v, %v; want nil, false", got, ok)
	}
}

// TestCloneIsolatedUnderConcurrentWrites is the property snapshots rely on: a
// goroutine reads a clone, without any lock, while the original is rewritten.
// If a write ever touched a node the clone can see, -race reports a data race
// (whatever the timing), or the reader sees a value other than "old".
func TestCloneIsolatedUnderConcurrentWrites(t *testing.T) {
	const n = 1000
	keyOf := func(i int) []byte { return fmt.Appendf(nil, "key-%04d", i) }

	m := NewMap()
	for i := range n {
		m.Put(keyOf(i), []byte("old"))
	}
	snap := m.Clone()

	// The test owns this goroutine. It exits after a fixed number of passes and
	// always sends exactly one result on the buffered channel, so it can't leak.
	done := make(chan error, 1)
	go func() {
		for range 20 {
			seen := 0
			var bad error
			snap.Ascend(func(key, value []byte) bool {
				if string(value) != "old" {
					bad = fmt.Errorf("clone saw %q = %q, want \"old\"", key, value)
					return false
				}
				seen++
				return true
			})
			if bad == nil && seen != n {
				bad = fmt.Errorf("clone has %d keys, want %d", seen, n)
			}
			if bad != nil {
				done <- bad
				return
			}
		}
		done <- nil
	}()

	for i := range n {
		m.Put(keyOf(i), []byte("new"))
		if i%3 == 0 {
			m.Delete(keyOf(i))
		}
	}
	m.Put([]byte("added-after-clone"), []byte("x"))

	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Get([]byte("added-after-clone")); ok {
		t.Fatal("clone sees a key added to the original after Clone")
	}
}
