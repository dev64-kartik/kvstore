package main

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dev64-kartik/kvstore/api/kvpb"
	"github.com/dev64-kartik/kvstore/internal/server"
)

// blockingStore holds every Put until release is closed (or its ctx ends).
type blockingStore struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingStore) Get([]byte) ([]byte, error)           { return nil, nil }
func (b *blockingStore) Delete(context.Context, []byte) error { return nil }
func (b *blockingStore) Put(ctx context.Context, _, _ []byte) error {
	close(b.entered)
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startBlocked serves a blockingStore and starts one Put, returning once that
// Put is inside the store.
func startBlocked(t *testing.T) (*grpc.Server, *blockingStore, <-chan error) {
	t.Helper()
	st := &blockingStore{entered: make(chan struct{}), release: make(chan struct{})}
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(server.Options()...)
	kvpb.RegisterKVServiceServer(g, server.New(st, slog.New(slog.DiscardHandler)))
	go func() { _ = g.Serve(lis) }() //nolint:errcheck // nil or ErrServerStopped once the test stops it; nothing to report
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Stop(); _ = conn.Close() }) //nolint:errcheck // teardown
	result := make(chan error, 1)
	go func() {
		_, err := kvpb.NewKVServiceClient(conn).Put(context.Background(), &kvpb.PutRequest{Key: []byte("k")})
		result <- err
	}()
	<-st.entered
	return g, st, result
}

// An RPC in flight when shutdown starts runs to completion: shutdown waits
// for it, it isn't cut off.
func TestGracefulStopDrainsInFlightRPCs(t *testing.T) {
	g, st, result := startBlocked(t)
	stopped := make(chan struct{})
	go func() {
		gracefulStop(g, time.Hour, slog.New(slog.DiscardHandler))
		close(stopped)
	}()
	// A weak check (gracefulStop may not have got far yet), but it can't
	// fail falsely: while the RPC is held, neither may have finished. The
	// strong check is that the RPC below succeeds.
	select {
	case <-stopped:
		t.Fatal("gracefulStop returned while an RPC was still running")
	case err := <-result:
		t.Fatalf("in-flight RPC ended before it was released: %v", err)
	default:
	}
	close(st.release)
	if err := <-result; err != nil {
		t.Fatalf("in-flight Put during graceful stop: %v, want success", err)
	}
	<-stopped
}

// Past the timeout, the stuck RPC is cancelled and shutdown completes.
func TestGracefulStopCancelsAfterTimeout(t *testing.T) {
	g, _, result := startBlocked(t)
	gracefulStop(g, 10*time.Millisecond, slog.New(slog.DiscardHandler))
	if err := <-result; status.Code(err) == codes.OK {
		t.Fatal("stuck RPC succeeded; want it cancelled by Stop")
	}
}

// Regression: a shutdown signal that arrives before the Serve goroutine runs
// made Serve return grpc.ErrServerStopped, and run reported it, so a clean
// SIGTERM could exit with status 1. A ctx that is already cancelled makes
// shutdown race Serve on every iteration. The race isn't certain on any one
// run, so it repeats; before the fix this failed within a few iterations.
func TestImmediateShutdownIsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := range 50 {
		if err := run(ctx, []string{"-dir", t.TempDir(), "-addr", "127.0.0.1:0"}, slog.New(slog.DiscardHandler)); err != nil {
			t.Fatalf("iteration %d: run with an already-cancelled ctx: %v, want a clean shutdown", i, err)
		}
	}
}
