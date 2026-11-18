package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dev64-kartik/kvstore/pkg/client"
)

// The binary tests re-run this test binary with childEnv set; TestMain then
// runs the real main() (flags, signals, exit code) instead of the tests.
const childEnv = "KVNODE_TEST_CHILD"

func TestMain(m *testing.M) {
	if os.Getenv(childEnv) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

var seedFlag = flag.Uint64("seed", 0, "seed for randomized tests (0 = pick one)")

// bounded is the most any wait on the child may take: a deadline to fail
// with a message, never a sleep.
const bounded = 30 * time.Second

// proc is a running kvnode child.
type proc struct {
	cmd     *exec.Cmd
	addr    string
	exited  chan struct{} // closed once the child has exited and been reaped
	waitErr error         // cmd.Wait's result; read only after exited is closed

	mu     sync.Mutex
	stderr bytes.Buffer // everything the child logged
}

func (p *proc) log() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stderr.String()
}

var listening = regexp.MustCompile(`msg="kvnode: listening" addr=(\S+)`)

// start runs kvnode with args. If wantListen, it waits for the "listening"
// line and returns the bound address; otherwise it only starts the process.
func start(t *testing.T, wantListen bool, args ...string) *proc {
	t.Helper()
	p := &proc{cmd: exec.Command(os.Args[0], args...), exited: make(chan struct{})}
	p.cmd.Env = append(os.Environ(), childEnv+"=1")
	pipe, err := p.cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	addrs := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(pipe)
		for sc.Scan() {
			p.mu.Lock()
			p.stderr.WriteString(sc.Text() + "\n")
			p.mu.Unlock()
			if m := listening.FindStringSubmatch(sc.Text()); m != nil {
				addrs <- m[1]
			}
		}
		p.waitErr = p.cmd.Wait() // only after stderr is fully read, as Wait requires
		close(p.exited)
	}()
	t.Cleanup(func() {
		// Never leave a child running. Kill after exit returns ErrProcessDone.
		if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
		<-p.exited
	})
	if !wantListen {
		return p
	}
	select {
	case p.addr = <-addrs:
	case <-p.exited:
		t.Fatalf("kvnode exited before listening: %v\n%s", p.waitErr, p.log())
	case <-time.After(bounded):
		t.Fatalf("kvnode didn't start listening\n%s", p.log())
	}
	return p
}

// wait returns the child's exit error (nil for exit status 0).
func (p *proc) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-p.exited:
		return p.waitErr
	case <-time.After(bounded):
		t.Fatalf("kvnode didn't exit\n%s", p.log())
		return nil
	}
}

func dial(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.New(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}

func nodeArgs(dir string) []string { return []string{"-dir", dir, "-addr", "127.0.0.1:0"} }

// Start, write, see the snapshot ticker produce a snapshot, SIGTERM: clean
// exit 0. Restart on the same dir: every write is there.
func TestSIGTERMShutsDownCleanly(t *testing.T) {
	dir := t.TempDir()
	p := start(t, true, append(nodeArgs(dir), "-snapshot-interval", "1ms")...)
	c := dial(t, p.addr)
	ctx := context.Background()
	for i := range 20 {
		if err := c.Put(ctx, fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Poll with a deadline (not a sleep) until the ticker has snapshotted.
	for deadline := time.Now().Add(bounded); ; {
		if snaps, err := filepath.Glob(filepath.Join(dir, "snap-*.snap")); err != nil || len(snaps) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no snapshot after %v with -snapshot-interval 1ms\n%s", bounded, p.log())
		}
		runtime.Gosched()
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := p.wait(t); err != nil {
		t.Fatalf("exit after SIGTERM: %v, want status 0\n%s", err, p.log())
	}
	if !strings.Contains(p.log(), "kvnode: shutting down") {
		t.Fatalf("no shutdown log line\n%s", p.log())
	}

	p = start(t, true, nodeArgs(dir)...)
	c = dial(t, p.addr)
	for i := range 20 {
		if v, err := c.Get(ctx, fmt.Appendf(nil, "k%d", i)); err != nil || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("after restart, k%d = %q, %v", i, v, err)
		}
	}
}

// A data dir that can't be recovered: non-zero exit, and the port never
// opens, so no client could have read from it.
func TestUnrecoverableDirIsNeverServed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "snap-00000000000000000007.snap"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"corrupt snapshot": nodeArgs(dir),
		"missing -dir":     {"-addr", "127.0.0.1:0"},
		"missing dir":      nodeArgs(filepath.Join(dir, "nope")),
	} {
		p := start(t, false, args...)
		var exit *exec.ExitError
		if err := p.wait(t); !errors.As(err, &exit) || exit.ExitCode() != 1 {
			t.Errorf("%s: exit %v, want status 1\n%s", name, err, p.log())
		}
		if listening.MatchString(p.log()) {
			t.Errorf("%s: kvnode listened anyway\n%s", name, p.log())
		}
	}
}

// SIGKILL a real node while a client writes to it, at a random point, over
// several restarts of the same dir. The client's view is the truth here:
// every write it saw succeed must survive, and the recovered state must be
// what the writes up to some point produce. A write in flight at the kill
// may or may not be there.
func TestSIGKILLWhileWriting(t *testing.T) {
	seed := *seedFlag
	if seed == 0 {
		seed = rand.Uint64()
	}
	t.Logf("seed %d (replay with -args -seed=%d)", seed, seed)
	rng := rand.New(rand.NewPCG(seed, 0))
	dir := t.TempDir()
	durable := map[string]string{}

	for round := range 5 {
		p := start(t, true, append(nodeArgs(dir), "-snapshot-interval", "1ms")...) // snapshots run constantly
		c := dial(t, p.addr)
		state := maps.Clone(durable) // what the acknowledged writes produced
		killAt := rng.IntN(60)

		var inflight struct{ key, value string }
		for i := 0; ; i++ {
			key, value := fmt.Sprintf("k%d", rng.IntN(15)), fmt.Sprintf("r%d-w%d", round, i)
			if i < killAt {
				if err := c.Put(context.Background(), []byte(key), []byte(value)); err != nil {
					t.Fatalf("round %d write %d: %v\n%s", round, i, err, p.log())
				}
				state[key] = value
				continue
			}
			// Write killAt races the kill.
			inflight.key, inflight.value = key, value
			done := make(chan error, 1)
			go func() { done <- c.Put(context.Background(), []byte(key), []byte(value)) }()
			if err := p.cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err == nil {
				state[key] = value // acknowledged before the kill landed
			} else if !errors.Is(err, client.ErrIndeterminate) {
				t.Fatalf("round %d: in-flight write: %v, want nil or ErrIndeterminate", round, err)
			}
			break
		}
		if err := p.wait(t); err == nil {
			t.Fatalf("round %d: killed node exited with status 0", round)
		}

		p = start(t, true, nodeArgs(dir)...)
		got := readAll(t, dial(t, p.addr))
		withInflight := maps.Clone(state)
		withInflight[inflight.key] = inflight.value
		if !maps.Equal(got, state) && !maps.Equal(got, withInflight) {
			t.Fatalf("round %d (killed at write %d): recovered %v\nwant %v\n  or %v", round, killAt, got, state, withInflight)
		}
		durable = got
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := p.wait(t); err != nil {
			t.Fatalf("round %d: SIGTERM after recovery: %v\n%s", round, err, p.log())
		}
	}
}

func readAll(t *testing.T, c *client.Client) map[string]string {
	t.Helper()
	m := map[string]string{}
	for k := range 15 {
		key := fmt.Sprintf("k%d", k)
		v, err := c.Get(context.Background(), []byte(key))
		switch {
		case err == nil:
			m[key] = string(v)
		case !errors.Is(err, client.ErrNotFound):
			t.Fatalf("Get(%s): %v", key, err)
		}
	}
	return m
}

func TestSnapshotLoop(t *testing.T) {
	tick := make(chan time.Time)
	calls := make(chan struct{}, 10)
	fail := errors.New("injected")
	snapshot := func(context.Context) error {
		calls <- struct{}{}
		return fail // a failure must not stop the loop
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		snapshotLoop(ctx, tick, snapshot, slog.New(slog.DiscardHandler))
		close(done)
	}()
	for i := range 3 {
		// Unbuffered, so the send completes only when the loop takes the tick.
		// If the loop had quit after the first failure, this would block, so
		// bound it.
		select {
		case tick <- time.Time{}:
		case <-time.After(bounded):
			t.Fatalf("tick %d not taken: the loop stopped after a failed snapshot", i)
		}
		<-calls
	}
	cancel()
	<-done // the loop returns when ctx ends
	if len(calls) != 0 {
		t.Fatalf("%d extra snapshot calls", len(calls))
	}
}
