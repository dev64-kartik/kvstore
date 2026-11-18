package client_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dev64-kartik/kvstore/api/kvpb"
	"github.com/dev64-kartik/kvstore/internal/localdb"
	"github.com/dev64-kartik/kvstore/internal/server"
	"github.com/dev64-kartik/kvstore/internal/store"
	"github.com/dev64-kartik/kvstore/internal/vfs/crashfs"
	"github.com/dev64-kartik/kvstore/pkg/client"
)

var (
	seedFlag = flag.Uint64("seed", 0, "seed for randomized tests (0 = pick one)")
	discard  = slog.New(slog.DiscardHandler)
)

// node is a server for st on an in-memory listener. stop shuts it down and
// waits for its Serve goroutine; it is also registered as a cleanup.
type node struct {
	lis     *bufconn.Listener
	stopped chan struct{}
	g       *grpc.Server
}

func startNode(t *testing.T, st server.Store) *node {
	t.Helper()
	n := &node{lis: bufconn.Listen(1 << 20), stopped: make(chan struct{}), g: grpc.NewServer(server.Options()...)}
	kvpb.RegisterKVServiceServer(n.g, server.New(st, discard))
	go func() {
		defer close(n.stopped)
		if err := n.g.Serve(n.lis); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()
	t.Cleanup(n.stop)
	return n
}

func (n *node) stop() {
	n.g.Stop() // idempotent
	<-n.stopped
}

// connect returns a client for n, reached through its in-memory listener.
func connect(t *testing.T, n *node) *client.Client {
	t.Helper()
	c, err := client.New("passthrough:///bufnet", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return n.lis.DialContext(ctx)
	}))
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

func openDB(t *testing.T, fsys *crashfs.FS) *localdb.DB {
	t.Helper()
	db, err := localdb.Open("/db", localdb.Options{FS: fsys, Logger: discard})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// Random ops through client → gRPC → server → localdb, against a map. Then a
// simulated power loss and a fresh node on the same disk: everything the
// client was told succeeded must still be there, read back over the network.
func TestClientMatchesModelAcrossCrash(t *testing.T) {
	seed := *seedFlag
	if seed == 0 {
		seed = rand.Uint64()
	}
	t.Logf("seed %d (replay with -args -seed=%d)", seed, seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	fsys := crashfs.New()
	fsys.Mkdir("/db")
	db := openDB(t, fsys)
	n := startNode(t, db)
	c := connect(t, n)
	ctx := context.Background()
	model := map[string][]byte{}

	for i := range 500 {
		key := []byte(fmt.Sprintf("k%d", rng.IntN(30)))
		if rng.IntN(4) == 0 {
			if err := c.Delete(ctx, key); err != nil {
				t.Fatalf("op %d: Delete: %v", i, err)
			}
			delete(model, string(key))
			continue
		}
		value := make([]byte, rng.IntN(100)) // includes empty values
		for j := range value {
			value[j] = byte(rng.Uint32())
		}
		if err := c.Put(ctx, key, value); err != nil {
			t.Fatalf("op %d: Put: %v", i, err)
		}
		model[string(key)] = value
	}

	// Always end with one empty value: random values are empty only 1% of
	// the time, so the nil-vs-empty check below would rarely run otherwise.
	if err := c.Put(ctx, []byte("k0"), nil); err != nil {
		t.Fatal(err)
	}
	model["k0"] = []byte{}

	n.stop()
	fsys.Crash(crashfs.DropUnsynced()) // no db.Close: power loss
	db = openDB(t, fsys)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	// A new client: the old one's connection broke with the old node, and a
	// fail-fast call during its reconnect backoff would fail for that reason.
	c = connect(t, startNode(t, db))
	for k := range 30 {
		key := fmt.Sprintf("k%d", k)
		got, err := c.Get(ctx, []byte(key))
		want, ok := model[key]
		switch {
		case !ok && !errors.Is(err, client.ErrNotFound):
			t.Fatalf("Get(%s) = %q, %v; want ErrNotFound", key, got, err)
		case ok && (err != nil || got == nil || !bytes.Equal(got, want)):
			t.Fatalf("Get(%s) = %q, %v; want %q (non-nil)", key, got, err, want)
		}
	}
}

// fakeStore returns err from every call, after calling hook (if set).
type fakeStore struct {
	err  error
	hook func(ctx context.Context)
}

func (f *fakeStore) call(ctx context.Context) error {
	if f.hook != nil {
		f.hook(ctx)
	}
	return f.err
}
func (f *fakeStore) Get([]byte) ([]byte, error)                 { return nil, f.call(context.Background()) }
func (f *fakeStore) Put(ctx context.Context, _, _ []byte) error { return f.call(ctx) }
func (f *fakeStore) Delete(ctx context.Context, _ []byte) error { return f.call(ctx) }

// The design doc's error table, end to end: each store error reaches the
// caller as the right outcome, with the gRPC status still inside.
func TestWriteOutcomes(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		want     error // nil, ErrInvalidArgument or ErrIndeterminate
		wantCode codes.Code
	}{
		{"applied", nil, nil, codes.OK},
		{"rejected", fmt.Errorf("%w: key too long", store.ErrInvalidArgument), client.ErrInvalidArgument, codes.InvalidArgument},
		{"WAL failed", localdb.ErrFailed, client.ErrIndeterminate, codes.Unavailable},
		{"shutting down", localdb.ErrClosed, client.ErrIndeterminate, codes.Unavailable},
		{"store gave up waiting", context.DeadlineExceeded, client.ErrIndeterminate, codes.DeadlineExceeded},
		{"unexpected", errors.New("boom"), client.ErrIndeterminate, codes.Internal},
	}
	for _, tt := range tests {
		n := startNode(t, &fakeStore{err: tt.err})
		c := connect(t, n)
		for method, err := range map[string]error{
			"Put":    c.Put(context.Background(), []byte("k"), []byte("v")),
			"Delete": c.Delete(context.Background(), []byte("k")),
		} {
			if tt.want == nil && err != nil || tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("%s, %s: err = %v, want %v", tt.name, method, err, tt.want)
			}
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("%s, %s: status code %v, want %v (still inspectable)", tt.name, method, got, tt.wantCode)
			}
		}
	}
}

func TestGetOutcomes(t *testing.T) {
	n := startNode(t, &fakeStore{err: store.ErrNotFound})
	c := connect(t, n)
	if v, err := c.Get(context.Background(), []byte("k")); !errors.Is(err, client.ErrNotFound) || v != nil {
		t.Fatalf("Get of a missing key = %q, %v; want ErrNotFound", v, err)
	}
	n2 := startNode(t, &fakeStore{err: localdb.ErrClosed})
	c2 := connect(t, n2)
	if _, err := c2.Get(context.Background(), []byte("k")); !errors.Is(err, client.ErrIndeterminate) {
		t.Fatalf("Get from a closing node: %v, want ErrIndeterminate", err)
	}
}

// Oversized requests are cut off by gRPC's size limit (ResourceExhausted)
// before the store sees them; that still proves "not applied".
func TestOversizedRequestIsRejected(t *testing.T) {
	n := startNode(t, &fakeStore{})
	c := connect(t, n)
	err := c.Put(context.Background(), []byte("k"), make([]byte, server.MaxRequestSize))
	if !errors.Is(err, client.ErrInvalidArgument) || status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized Put: %v, want ErrInvalidArgument wrapping ResourceExhausted", err)
	}
}

// The caller's own cancel mid-write: indeterminate, and errors.Is finds the
// context error, as Go callers expect.
func TestCancelledWriteIsIndeterminate(t *testing.T) {
	entered := make(chan struct{})
	n := startNode(t, &fakeStore{hook: func(ctx context.Context) {
		close(entered)
		<-ctx.Done()
	}})
	c := connect(t, n)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Put(ctx, []byte("k"), []byte("v")) }()
	<-entered // the write has reached the store: cancelling now can't prove anything
	cancel()
	err := <-done
	if !errors.Is(err, client.ErrIndeterminate) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Put: %v, want ErrIndeterminate and context.Canceled", err)
	}
}

// With the server gone, calls fail fast (no waiting for it to come back) and
// are indeterminate: a client can't tell "never sent" from "sent, then lost".
func TestServerDownFailsFastAndIndeterminate(t *testing.T) {
	n := startNode(t, &fakeStore{})
	c := connect(t, n)
	n.stop()
	if err := n.lis.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // a bound, not a sleep
	defer cancel()
	err := c.Put(ctx, []byte("k"), []byte("v"))
	if !errors.Is(err, client.ErrIndeterminate) || status.Code(err) != codes.Unavailable {
		t.Fatalf("Put to a stopped server: %v, want ErrIndeterminate with Unavailable", err)
	}
}
