package localdb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs/crashfs"
)

// Shared by the crashfs and SIGKILL tests: a deterministic op sequence, and a
// check that a recovered state is the result of a prefix of it.

type opKind int

const (
	opPut opKind = iota
	opDelete
	opSnapshot
)

type op struct {
	kind  opKind
	key   string
	value string
}

const crashKeys = 20

// genOps returns n ops over a small key space, so keys are overwritten and
// deleted often. Every Put value is unique (it embeds the op's index), so a
// recovered value identifies exactly which op wrote it.
func genOps(rng *rand.Rand, n int) []op {
	ops := make([]op, n)
	for i := range ops {
		key := fmt.Sprintf("k%d", rng.IntN(crashKeys))
		switch r := rng.IntN(100); {
		case r < 3:
			ops[i] = op{kind: opSnapshot}
		case r < 25:
			ops[i] = op{kind: opDelete, key: key}
		default:
			// Sizes up to 3 KiB, so records straddle the 512-byte sectors
			// that crashfs tears at.
			pad := make([]byte, rng.IntN(3<<10))
			for j := range pad {
				pad[j] = 'a' + byte(rng.IntN(26))
			}
			ops[i] = op{kind: opPut, key: key, value: fmt.Sprintf("op%d:%s", i, pad)}
		}
	}
	return ops
}

func (o op) run(ctx context.Context, db *DB) error {
	switch o.kind {
	case opPut:
		return db.Put(ctx, []byte(o.key), []byte(o.value))
	case opDelete:
		return db.Delete(ctx, []byte(o.key))
	default:
		return db.Snapshot(ctx)
	}
}

type state map[string]string

func (s state) apply(o op) {
	switch o.kind {
	case opPut:
		s[o.key] = o.value
	case opDelete:
		delete(s, o.key)
	}
}

// readState reads every key in the key space.
func readState(t *testing.T, db *DB) state {
	t.Helper()
	s := state{}
	for k := range crashKeys {
		key := fmt.Sprintf("k%d", k)
		v, err := db.Get([]byte(key))
		switch {
		case err == nil:
			s[key] = string(v)
		case !errors.Is(err, ErrNotFound):
			t.Fatalf("Get(%q): %v", key, err)
		}
	}
	return s
}

// matchPrefix returns the p in [lo, hi] for which got equals base with
// ops[:p] applied, or -1. The WAL is applied in order, so a correct recovery
// always equals some prefix of the ops; lo is the number acknowledged, which
// must all be there, and nothing past hi was ever issued.
func matchPrefix(base state, ops []op, lo, hi int, got state) int {
	s := maps.Clone(base)
	for _, o := range ops[:lo] {
		s.apply(o)
	}
	for p := lo; p <= hi; p++ {
		if maps.Equal(s, got) {
			return p
		}
		if p < len(ops) {
			s.apply(ops[p])
		}
	}
	return -1
}

func describe(base state, ops []op, acked int, got state) string {
	want := durableAfter(base, ops, acked)
	var diff []string
	for k := range crashKeys {
		key := fmt.Sprintf("k%d", k)
		if g, w := got[key], want[key]; g != w {
			diff = append(diff, fmt.Sprintf("%s: got %.12q, acked state has %.12q", key, g, w))
		}
	}
	return fmt.Sprint(diff)
}

// Random ops, a crash at a random filesystem op (possibly inside Open's own
// recovery) with a random crash policy, repeated over cycles on the same
// filesystem. After each recovery the state must be the durable state from
// before, plus every acknowledged op, plus at most the one op in flight.
// What a recovery returns then becomes the base for the next cycle, so a
// write that surfaced once can never disappear later.
func TestRandomCrashes(t *testing.T) {
	base := testSeed(t)
	// Where the crashes landed, so a change that stops exercising some phase
	// shows up in the log rather than passing silently.
	var inOpen, midOps, afterOps, inflightSurvived int
	for i := range uint64(100) {
		rng := rand.New(rand.NewPCG(base, i))
		fsys := newFS()
		durable := state{}

		for cycle := range 6 {
			ops := genOps(rng, 1+rng.IntN(60))
			crashAt, fsOps := 1+rng.IntN(150), 0 // a cycle does roughly 2 fs ops per write, plus Open and snapshots
			policy := crashfs.Random(rng)
			fsys.SetHook(func(string, string) error {
				if fsOps++; fsOps == crashAt {
					fsys.Crash(policy)
					return crashfs.ErrCrashed
				}
				return nil
			})

			acked := 0
			if db, err := Open(testDir, Options{FS: fsys, Logger: discard}); err != nil {
				if !errors.Is(err, crashfs.ErrCrashed) {
					t.Fatalf("seed %d/%d cycle %d: Open: %v", base, i, cycle, err)
				}
				ops = nil // crashed during recovery: nothing was issued
				inOpen++
			} else {
				for _, o := range ops {
					if err := o.run(context.Background(), db); err != nil {
						if !errors.Is(err, crashfs.ErrCrashed) {
							t.Fatalf("seed %d/%d cycle %d op %d: %v", base, i, cycle, acked, err)
						}
						break
					}
					acked++
				}
			}
			if fsOps < crashAt {
				fsys.Crash(policy) // the ops finished first: crash now
				afterOps++
			} else if ops != nil {
				midOps++
			}
			fsys.SetHook(nil)

			db := mustOpen(t, fsys)
			got := readState(t, db)
			p := matchPrefix(durable, ops, acked, min(acked+1, len(ops)), got)
			if p < 0 {
				t.Fatalf("seed %d/%d cycle %d: recovered state isn't a prefix of the ops (%d acked): %s",
					base, i, cycle, acked, describe(durable, ops, acked, got))
			}
			if p > acked && !maps.Equal(got, durableAfter(durable, ops, acked)) {
				inflightSurvived++
			}
			durable = got
			mustClose(t, db)
		}
	}
	t.Logf("crashes: %d in Open, %d mid-ops, %d after ops; in-flight write survived %d times",
		inOpen, midOps, afterOps, inflightSurvived)
}

func durableAfter(base state, ops []op, n int) state {
	s := maps.Clone(base)
	for _, o := range ops[:n] {
		s.apply(o)
	}
	return s
}
