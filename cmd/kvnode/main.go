// Command kvnode runs a single kvstore node: it recovers the data directory,
// then serves the KV API over gRPC until SIGINT or SIGTERM.
//
//	kvnode -dir DIR [-addr 127.0.0.1:7070] [-snapshot-interval 5m]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/dev64-kartik/kvstore/api/kvpb"
	"github.com/dev64-kartik/kvstore/internal/localdb"
	"github.com/dev64-kartik/kvstore/internal/server"
)

// shutdownTimeout bounds how long in-flight RPCs get to finish on shutdown.
const shutdownTimeout = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	err := run(ctx, os.Args[1:], log)
	stop()
	if err != nil {
		log.Error("kvnode: exiting", "err", err)
		os.Exit(1)
	}
}

// run is the whole node; main only adds signals and the exit code. It
// returns when ctx ends (after a clean shutdown) or when serving fails.
func run(ctx context.Context, args []string, log *slog.Logger) (err error) {
	fs := flag.NewFlagSet("kvnode", flag.ContinueOnError)
	dir := fs.String("dir", "", "data directory (required, must exist)")
	addr := fs.String("addr", "127.0.0.1:7070", "listen address; localhost by default, since there is no TLS or auth")
	snapEvery := fs.Duration("snapshot-interval", 5*time.Minute, "how often to snapshot (a no-op if nothing changed)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *snapEvery <= 0 {
		fs.Usage()
		return errors.New("-dir is required and -snapshot-interval must be positive")
	}

	// Recovery first. The port opens only after it succeeds, so no client
	// can ever read a partially recovered state, and a data directory we
	// can't recover is never served at all.
	db, err := localdb.Open(*dir, localdb.Options{Logger: log})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }() // step 3 of shutdown: last

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	g := grpc.NewServer(server.Options()...)
	kvpb.RegisterKVServiceServer(g, server.New(db, log))
	served := make(chan error, 1)
	go func() { served <- g.Serve(lis) }() // Serve closes lis when it returns
	log.Info("kvnode: listening", "addr", lis.Addr().String(), "dir", *dir)

	tickerCtx, stopTicker := context.WithCancel(context.Background())
	tickerDone := make(chan struct{})
	go func() {
		defer close(tickerDone)
		ticker := time.NewTicker(*snapEvery)
		defer ticker.Stop()
		snapshotLoop(tickerCtx, ticker.C, db.Snapshot, log)
	}()

	select {
	case <-ctx.Done():
		log.Info("kvnode: shutting down")
	case err = <-served:
		err = fmt.Errorf("serve: %w", err)
		served <- nil // so the drain below doesn't block
	}

	// Shutdown order matters: drain RPCs while the DB is still open, or
	// in-flight writes would fail with ErrClosed. Then stop the ticker (a
	// snapshot in progress sees its ctx cancelled), then close the DB.
	gracefulStop(g, shutdownTimeout, log)
	// ErrServerStopped means the shutdown reached gRPC before the goroutine
	// reached Serve: a signal right after startup. That's a clean shutdown,
	// not a serving error (TestImmediateShutdownIsClean).
	if serveErr := <-served; err == nil && serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
		err = fmt.Errorf("serve: %w", serveErr)
	}
	stopTicker()
	<-tickerDone
	return err
}

// gracefulStop lets in-flight RPCs finish, up to timeout. After that, Stop
// cancels the rest. Cancelling is safe even for a write: its ctx is honored
// only while it waits for the writer slot, never mid-WAL-append, so the
// worst outcome is a write the client sees as indeterminate.
func gracefulStop(g *grpc.Server, timeout time.Duration, log *slog.Logger) {
	done := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		log.Warn("kvnode: RPCs still running after the shutdown timeout; cancelling them", "timeout", timeout)
		g.Stop()
		<-done // GracefulStop returns once Stop has closed everything
	}
}

// snapshotLoop calls snapshot on every tick until ctx ends. A failure is
// logged and retried on the next tick: a failed snapshot leaves the DB
// writable (Phase 1 design). Raft takes this job over in Phase 3.
func snapshotLoop(ctx context.Context, tick <-chan time.Time, snapshot func(context.Context) error, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			if err := snapshot(ctx); err != nil && ctx.Err() == nil {
				log.Error("kvnode: snapshot failed; retrying at the next tick", "err", err)
			}
		}
	}
}
