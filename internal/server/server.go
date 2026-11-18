// Package server is the gRPC front end of a node: it decodes requests, calls
// the store, and turns the store's errors into status codes that tell the
// client whether a write might have happened (design doc, "Error model").
package server

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dev64-kartik/kvstore/api/kvpb"
	"github.com/dev64-kartik/kvstore/internal/localdb"
	"github.com/dev64-kartik/kvstore/internal/store"
)

// Store is what the server needs from the storage layer. *localdb.DB
// satisfies it today; the Raft-backed shard will in Phase 3.
//
// Get must return store.ErrNotFound for a missing key, and every method must
// return store.ErrInvalidArgument (possibly wrapped) for out-of-range input.
type Store interface {
	Get(key []byte) ([]byte, error)
	Put(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
}

// MaxRequestSize caps how large a request gRPC will read: the largest valid
// key and value plus room for framing. Without it gRPC buffers up to 4 MiB
// per request before the store gets to reject it. The store still checks the
// exact limits; this only bounds memory.
const MaxRequestSize = store.MaxKeySize + store.MaxValueSize + 4<<10

// Options returns the grpc.ServerOptions the KV service relies on.
func Options() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.MaxRecvMsgSize(MaxRequestSize)}
}

// Server implements kvpb.KVServiceServer.
type Server struct {
	// Embedded by value, as the generated code requires: if the proto gains
	// an RPC, this supplies a method returning codes.Unimplemented, so the
	// server still compiles.
	kvpb.UnimplementedKVServiceServer

	store Store
	log   *slog.Logger
}

// New returns a Server backed by st. Register it with
// kvpb.RegisterKVServiceServer on a grpc.Server created with Options().
func New(st Store, log *slog.Logger) *Server {
	return &Server{store: st, log: log}
}

// Get reports a missing key as found = false, not as an error: "not there"
// is an answer, and a status error would be counted as a failed request.
func (s *Server) Get(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	v, err := s.store.Get(req.GetKey())
	if errors.Is(err, store.ErrNotFound) {
		return &kvpb.GetResponse{Found: false}, nil
	}
	if err != nil {
		return nil, s.toStatus("Get", err)
	}
	return &kvpb.GetResponse{Found: true, Value: v}, nil
}

// Put passes the request's ctx to the store, so the client's deadline is
// honored while the write waits for the writer, and not once its WAL append
// has started.
func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := s.store.Put(ctx, req.GetKey(), req.GetValue()); err != nil {
		return nil, s.toStatus("Put", err)
	}
	return &kvpb.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if err := s.store.Delete(ctx, req.GetKey()); err != nil {
		return nil, s.toStatus("Delete", err)
	}
	return &kvpb.DeleteResponse{}, nil
}

// toStatus maps a store error to a gRPC status. Only InvalidArgument tells
// the client that a write was not applied; every other code leaves the
// outcome unknown, which is the truth for each of them.
func (s *Server) toStatus(method string, err error) error {
	switch {
	case errors.Is(err, store.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// The write gave up waiting for the writer slot, so it wasn't applied,
		// but the client can't tell this apart from its own deadline firing
		// mid-append, so the code carries no promise either way.
		return status.FromContextError(err).Err()
	case errors.Is(err, localdb.ErrFailed), errors.Is(err, localdb.ErrClosed):
		// This node can't take writes now. Unavailable is the code clients
		// read as "try elsewhere", which from Phase 3 on is possible.
		// Phase 3 replaces this case with the Raft shard's errors.
		return status.Error(codes.Unavailable, err.Error())
	default:
		s.log.Error("server: unexpected store error", "method", method, "err", err)
		return status.Error(codes.Internal, err.Error())
	}
}
