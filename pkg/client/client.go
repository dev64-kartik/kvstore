// Package client is the Go client for a kvstore node.
//
// Every write error says whether the write might have happened:
//   - nil: the write is durable.
//   - ErrInvalidArgument: the write was rejected and not applied.
//   - ErrIndeterminate: the write may or may not have been applied. This
//     covers deadlines, cancellation, server failures and network errors.
//     A server can't cancel a write once its log append has started, and a
//     dropped connection says nothing about whether the request arrived, so
//     no other answer would be honest.
//
// The underlying gRPC status is wrapped too, so status.Code(err) still
// works, and so does errors.Is(err, context.DeadlineExceeded) when the
// caller's ctx ended.
//
// One attempt per call: no retries (they come in Phase 6). This package
// imports only the public API, never internal/, so it works from outside
// the module.
package client

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/dev64-kartik/kvstore/api/kvpb"
)

var (
	// ErrNotFound: Get found no value for the key.
	ErrNotFound = errors.New("client: key not found")
	// ErrInvalidArgument: the server rejected the request (key or value out
	// of range). A write rejected this way was not applied.
	ErrInvalidArgument = errors.New("client: invalid argument")
	// ErrIndeterminate: no definite answer. For a write: it may or may not
	// have been applied. For a read: there is simply no result.
	ErrIndeterminate = errors.New("client: outcome unknown")
)

// Client talks to one node. It is safe for concurrent use.
type Client struct {
	conn *grpc.ClientConn
	kv   kvpb.KVServiceClient
}

// New returns a client for the node at addr ("host:port"). It doesn't
// connect yet: the connection is made on the first call, and remade if it
// drops. extra is appended to the defaults, so tests can add a dialer.
//
// No TLS: nodes listen on localhost only (Phase 2 design, non-goals).
func New(addr string, extra ...grpc.DialOption) (*Client, error) {
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, extra...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	return &Client{conn: conn, kv: kvpb.NewKVServiceClient(conn)}, nil
}

// Get returns the value for key, or ErrNotFound. A found value is never nil,
// even when it is empty, so an empty value can't be mistaken for a missing
// key.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	resp, err := c.kv.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		return nil, classify(ctx, err)
	}
	if !resp.GetFound() {
		return nil, ErrNotFound
	}
	if v := resp.GetValue(); v != nil {
		return v, nil
	}
	// proto3 decodes an empty bytes field as nil.
	return []byte{}, nil
}

// Put durably sets key to value. See the package doc for what each error
// says about whether the write happened.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	_, err := c.kv.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
	return classify(ctx, err)
}

// Delete durably removes key. Deleting a missing key succeeds.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	_, err := c.kv.Delete(ctx, &kvpb.DeleteRequest{Key: key})
	return classify(ctx, err)
}

// Close closes the connection. Calls in progress fail with ErrIndeterminate.
func (c *Client) Close() error {
	return c.conn.Close()
}

// classify turns a gRPC error into one of the package's three outcomes.
// Only codes that prove the request was rejected become ErrInvalidArgument;
// everything else, including codes we don't expect, is ErrIndeterminate. A
// new server-side code must therefore be added here deliberately before it
// can ever count as "not applied".
func classify(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.InvalidArgument,
		codes.ResourceExhausted: // the server's size limit, enforced before the handler runs
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// gRPC reports the caller's own deadline or cancel as a status, which
		// doesn't wrap the context error. Wrap it, so the usual
		// errors.Is(err, context.DeadlineExceeded) works.
		return fmt.Errorf("%w: %w: %w", ErrIndeterminate, ctxErr, err)
	}
	return fmt.Errorf("%w: %w", ErrIndeterminate, err)
}
