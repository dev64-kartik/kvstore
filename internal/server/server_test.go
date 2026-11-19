package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dev64-kartik/kvstore/api/kvpb"
	"github.com/dev64-kartik/kvstore/internal/localdb"
	"github.com/dev64-kartik/kvstore/internal/store"
	"github.com/dev64-kartik/kvstore/internal/vfs/crashfs"
)

var discard = slog.New(slog.DiscardHandler)

// serve runs a real gRPC server for st over an in-memory listener (no ports)
// and returns a client connected to it. Cleanup stops the server and waits
// for its Serve goroutine.
func serve(t *testing.T, st Store) kvpb.KVServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(Options()...)
	kvpb.RegisterKVServiceServer(g, New(st, discard))
	served := make(chan error, 1)
	go func() { served <- g.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		g.Stop()
		// Same race as in pkg/client's TestNodeStoppedBeforeServing.
		if err := <-served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Serve: %v", err)
		}
	})
	return kvpb.NewKVServiceClient(conn)
}

// fakeStore returns err from every method, after calling hook (if set) with
// the method's ctx.
type fakeStore struct {
	err   error
	value []byte
	hook  func(ctx context.Context)
	calls atomic.Int32 // handlers run on server goroutines
}

func (f *fakeStore) call(ctx context.Context) error {
	f.calls.Add(1)
	if f.hook != nil {
		f.hook(ctx)
	}
	return f.err
}

func (f *fakeStore) Get(key []byte) ([]byte, error) {
	return f.value, f.call(context.Background())
}
func (f *fakeStore) Put(ctx context.Context, _, _ []byte) error { return f.call(ctx) }
func (f *fakeStore) Delete(ctx context.Context, _ []byte) error { return f.call(ctx) }

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err  error
		want codes.Code
	}{
		{nil, codes.OK},
		{fmt.Errorf("%w: key too long", store.ErrInvalidArgument), codes.InvalidArgument},
		{fmt.Errorf("localdb: %w", localdb.ErrFailed), codes.Unavailable},
		{localdb.ErrClosed, codes.Unavailable},
		{context.Canceled, codes.Canceled},
		{fmt.Errorf("waiting: %w", context.DeadlineExceeded), codes.DeadlineExceeded},
		{errors.New("something unexpected"), codes.Internal},
	}
	for _, tt := range tests {
		f := &fakeStore{err: tt.err}
		c := serve(t, f)
		ctx := context.Background()
		_, putErr := c.Put(ctx, &kvpb.PutRequest{Key: []byte("k")})
		_, delErr := c.Delete(ctx, &kvpb.DeleteRequest{Key: []byte("k")})
		_, getErr := c.Get(ctx, &kvpb.GetRequest{Key: []byte("k")})
		for method, err := range map[string]error{"Put": putErr, "Delete": delErr, "Get": getErr} {
			if got := status.Code(err); got != tt.want {
				t.Errorf("%s with store error %v: code %v, want %v", method, tt.err, got, tt.want)
			}
		}
	}
}

// A missing key is a normal answer, and an empty value is not a missing key.
func TestGetFoundAndEmptyValues(t *testing.T) {
	fsys := crashfs.New()
	fsys.Mkdir("/db")
	db, err := localdb.Open("/db", localdb.Options{FS: fsys, Logger: discard})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	c := serve(t, db)
	ctx := context.Background()

	for key, value := range map[string][]byte{"empty": {}, "binary": {0, 0xff, 0}} {
		if _, err := c.Put(ctx, &kvpb.PutRequest{Key: []byte(key), Value: value}); err != nil {
			t.Fatal(err)
		}
		resp, err := c.Get(ctx, &kvpb.GetRequest{Key: []byte(key)})
		if err != nil || !resp.GetFound() || !bytes.Equal(resp.GetValue(), value) {
			t.Fatalf("Get(%q) = %v, %v; want found, %q", key, resp, err, value)
		}
	}
	resp, err := c.Get(ctx, &kvpb.GetRequest{Key: []byte("missing")})
	if err != nil || resp.GetFound() {
		t.Fatalf("Get(missing) = %v, %v; want found = false and no error", resp, err)
	}
	if _, err := c.Delete(ctx, &kvpb.DeleteRequest{Key: []byte("empty")}); err != nil {
		t.Fatal(err)
	}
	if resp, err := c.Get(ctx, &kvpb.GetRequest{Key: []byte("empty")}); err != nil || resp.GetFound() {
		t.Fatalf("Get after Delete = %v, %v", resp, err)
	}
	if _, err := c.Put(ctx, &kvpb.PutRequest{Value: []byte("v")}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put with empty key: %v, want InvalidArgument", err)
	}
}

// The client's cancellation and deadline must reach the store's ctx: that is
// what lets a write stop waiting for the writer slot.
func TestClientContextReachesStore(t *testing.T) {
	entered := make(chan time.Time, 1) // the deadline the store saw
	storeDone := make(chan error, 1)   // how the store's ctx ended
	f := &fakeStore{hook: func(ctx context.Context) {
		deadline, _ := ctx.Deadline()
		entered <- deadline
		<-ctx.Done() // blocks until the client cancels: no timing involved
		storeDone <- ctx.Err()
	}}
	c := serve(t, f)

	want := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), want)
	done := make(chan error, 1)
	go func() {
		_, err := c.Put(ctx, &kvpb.PutRequest{Key: []byte("k")})
		done <- err
	}()
	// The deadline travels as a relative timeout (grpc-timeout header), so
	// the server's copy is close to, not exactly, the client's.
	if deadline := <-entered; deadline.Sub(want).Abs() > time.Minute {
		t.Errorf("store saw deadline %v, want about %v", deadline, want)
	}
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("Put after cancel: %v, want Canceled", err)
	}
	// Bounded wait (a deadline, not a sleep): if the server dropped the
	// request's ctx, the store would wait forever, and this should fail with a
	// message rather than at the test timeout.
	select {
	case err := <-storeDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("store's ctx ended with %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the client's cancel never reached the store's ctx")
	}
}

func TestRequestSizeLimit(t *testing.T) {
	f := &fakeStore{}
	c := serve(t, f)
	ctx := context.Background()
	key, value := make([]byte, store.MaxKeySize), make([]byte, store.MaxValueSize)
	if _, err := c.Put(ctx, &kvpb.PutRequest{Key: key, Value: value}); err != nil {
		t.Fatalf("Put at the store's limits: %v", err)
	}
	big := make([]byte, MaxRequestSize)
	_, err := c.Put(ctx, &kvpb.PutRequest{Key: key, Value: big})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized Put: %v, want ResourceExhausted", err)
	}
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("store called %d times, want 1: the oversized request must not reach it", n)
	}
}
