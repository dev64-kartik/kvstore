package localdb

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// The SIGKILL test re-runs this test binary as a child process, selected by
// these environment variables, so parent and child share genOps.
const (
	childDirEnv  = "KVSTORE_SIGKILL_DIR"
	childSeedEnv = "KVSTORE_SIGKILL_SEED"
	childOps     = 120
)

// A real process on the real filesystem, killed with SIGKILL at a random
// point, possibly during its own recovery or a snapshot. The parent knows
// the child's ops (same seed) and reads its acknowledgements from a pipe.
// After each kill, the recovered state must be a prefix of the child's ops
// that includes every acknowledgement the parent saw.
//
// This is a process crash, not power loss: the OS keeps the page cache, so
// unsynced writes usually survive. It tests the real-disk code paths (os
// files, rename, directory listing) end to end; the crashfs tests cover
// power loss.
func TestSIGKILL(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes and does real fsyncs")
	}
	rng := rand.New(rand.NewPCG(testSeed(t), 0))
	dir := t.TempDir()
	durable := state{}

	for round := range 6 {
		childSeed := rng.Uint64()
		ops := genOps(rand.New(rand.NewPCG(childSeed, 0)), childOps)
		killAfter := rng.IntN(childOps + 1) // 0: kill as soon as it starts, maybe mid-recovery
		acked, stderr := runChildUntil(t, dir, childSeed, killAfter)

		db, err := Open(dir, Options{Logger: discard})
		if err != nil {
			t.Fatalf("round %d: Open after SIGKILL (%d acked): %v\nchild stderr:\n%s", round, acked, err, stderr)
		}
		got := readState(t, db)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		// The child may have acknowledged more than the parent read before the
		// kill, so anything from acked up to every op is allowed.
		if p := matchPrefix(durable, ops, acked, len(ops), got); p < 0 {
			t.Fatalf("round %d: recovered state isn't a prefix of the child's ops (%d acked): %s",
				round, acked, describe(durable, ops, acked, got))
		} else {
			t.Logf("round %d: killed after %d acks, recovered %d of %d ops", round, acked, p, len(ops))
		}
		durable = got
	}
}

// runChildUntil starts the child, reads acknowledgements until it has seen
// killAfter of them, then SIGKILLs it. The child may be anywhere after that
// ack: mid-write, mid-snapshot, or already exited after its last op.
func runChildUntil(t *testing.T, dir string, seed uint64, killAfter int) (acked int, stderr string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close() //nolint:errcheck // read end of a pipe; nothing to flush
	cmd := exec.Command(os.Args[0], "-test.run=^TestSIGKILLChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), childDirEnv+"="+dir, childSeedEnv+"="+strconv.FormatUint(seed, 10))
	cmd.ExtraFiles = []*os.File{w} // becomes fd 3 in the child
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Close our copy of the write end, so the read below sees EOF once the
	// child exits.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	sc := bufio.NewScanner(r)
	for acked < killAfter && sc.Scan() {
		line := sc.Text()
		n, err := strconv.Atoi(strings.TrimPrefix(line, "ack "))
		if err != nil || n != acked+1 {
			t.Fatalf("bad ack line %q after %d acks", line, acked)
		}
		acked = n
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}
	err = cmd.Wait() // an error is expected: we killed it
	// The pipe closing before killAfter acks means the child died on its own.
	if acked < killAfter {
		t.Fatalf("child exited after %d of %d acks: %v\n%s", acked, killAfter, err, errBuf.String())
	}
	return acked, errBuf.String()
}

// TestSIGKILLChild is the child half of TestSIGKILL; on its own it skips.
func TestSIGKILLChild(t *testing.T) {
	dir := os.Getenv(childDirEnv)
	if dir == "" {
		t.Skip("run by TestSIGKILL")
	}
	seed, err := strconv.ParseUint(os.Getenv(childSeedEnv), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	acks := os.NewFile(3, "acks")
	db, err := Open(dir, Options{Logger: discard})
	if err != nil {
		t.Fatal(err)
	}
	for i, o := range genOps(rand.New(rand.NewPCG(seed, 0)), childOps) {
		if err := o.run(context.Background(), db); err != nil {
			t.Fatalf("op %d: %v", i, err)
		}
		// Written only after the op returned: an ack the parent reads is a
		// promise the write is durable. Unbuffered, one write per line.
		if _, err := fmt.Fprintf(acks, "ack %d\n", i+1); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
