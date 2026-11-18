# Phase 2 design — gRPC server and Go client (single node)

Status: **APPROVED 2026-11-18**

## Package layout

| Package | Contents | Fate in Phase 3 |
|---|---|---|
| `api/kvpb` | `kv.proto` + generated Go code (checked in) | Kept; gains CAS and leader-redirect fields |
| `internal/server` | gRPC handlers: request → store call → status code | Kept; the store behind it becomes the Raft-backed shard |
| `pkg/client` | Go client: `Get`/`Put`/`Delete`, error classification | Kept; Phase 6 adds routing, retries, backoff |
| `cmd/kvnode` | Node binary: flags, recovery, serve, graceful shutdown, snapshot ticker | Kept; gains Raft config |
| `internal/store` | Gains the data-model errors and size limits (moved from `localdb`) | Kept |

## Goals

- A network API for the Phase 1 store: `Get`, `Put`, `Delete` over gRPC, served by a `kvnode` process.
- A small Go client library that hides gRPC and gives callers sentinel errors (`ErrNotFound`, …).
- **Acknowledgement means the same thing over the network as in-process**: an `OK` for a write means it is durable.
- **Every error says whether the write might have happened.** A client must never be told "failed" for a write
  that is in fact durable. This is the property Phase 6 retries and the Phase 8 linearizability checker rest on.
- Clean lifecycle: recover before serving; on SIGTERM, finish in-flight requests, then close the DB.
- End-to-end tests through the real binary, including `SIGKILL`.

## Non-goals (deliberately deferred)

| Not doing | Why |
|---|---|
| CAS | Phase 3, as its own Raft log entry type (settled decision). |
| Retries, backoff, redirects in the client | Phase 6. Phase 2's client makes exactly one attempt per call. |
| TLS, authentication | Local-only cluster. `kvnode` listens on `127.0.0.1` by default for that reason. |
| Metrics, tracing, request logging interceptors | Phase 7. Phase 2 logs lifecycle events only. |
| Streaming / batch RPCs, range scans | Single-key ops only. |
| gRPC health service, server reflection | Phase 6/7 (health matters once there are several nodes to route between). |
| Snapshot RPC / admin API | Snapshots are triggered inside the node (see below). |

## API

```proto
syntax = "proto3";
package kvstore.v1;
option go_package = "github.com/dev64-kartik/kvstore/api/kvpb";

service KVService {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Put(PutRequest) returns (PutResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}

message GetRequest    { bytes key = 1; }
message GetResponse   { bool found = 1; bytes value = 2; }
message PutRequest    { bytes key = 1; bytes value = 2; }
message PutResponse   {}
message DeleteRequest { bytes key = 1; }
message DeleteResponse {}
```

- **A missing key is a result, not an error**: `found = false` with status `OK`. Rejected alternative:
  `codes.NotFound`. A status error would be counted as a failure by every interceptor, metric and log line, and it
  would blur "the key isn't there" with "the request failed". The *client* still turns `found = false` into
  `client.ErrNotFound`, matching the Go API from Phase 1.
- `found` is needed because proto3 can't tell an empty `bytes` value from an absent one, and empty values are
  legal (edge case 1.19).
- The service is `KVService`, not `KV`: buf's standard lint rules require the `Service` suffix, and following
  them beats carrying exceptions.
- Empty response messages, rather than `google.protobuf.Empty`, so fields can be added later (e.g. a revision)
  without changing the RPC signature.
- Keys and values are `bytes`, not `string`: proto3 `string` must be valid UTF-8, and our keys are arbitrary bytes.

## Error model

The server maps store errors to gRPC status codes; the client maps them back.

| Store returns | gRPC code | Client returns | Write outcome |
|---|---|---|---|
| `nil` (Get: not found) | `OK`, `found=false` | `ErrNotFound` | — |
| `nil` | `OK` | `nil` | **applied, durable** |
| `ErrInvalidArgument` | `InvalidArgument` | `ErrInvalidArgument` | **not applied** |
| `ErrFailed` (WAL error) | `Unavailable` | wraps `ErrIndeterminate` | unknown |
| `ErrClosed` (shutting down) | `Unavailable` | wraps `ErrIndeterminate` | not applied (but the client can't know which case it is) |
| `ctx` cancelled / deadline | `Canceled` / `DeadlineExceeded` | wraps `ErrIndeterminate` | unknown |
| anything else | `Internal` (and logged) | wraps `ErrIndeterminate` | unknown |
| transport errors (connection refused, reset) | `Unavailable` etc. | wraps `ErrIndeterminate` | unknown |
| request too large | `ResourceExhausted` (before the handler runs) | `ErrInvalidArgument` | **not applied** |

**Why "indeterminate" is the default.** A write's WAL append is not cancellable (Phase 1 design), so a client whose
deadline fires may still have its write applied a moment later. A `Put` that hit a WAL error may have reached the
disk (edge case 1.11). A connection reset tells the client nothing about whether the server got the request. So
for writes, only `OK` and `InvalidArgument` are definite. Everything else is reported as `ErrIndeterminate`, with
the gRPC status still inside it (`errors.As` / `status.Code` work). Rejected alternative: return raw gRPC errors and
let callers figure it out. Every caller would have to re-derive the table above, and getting it wrong means
treating a durable write as failed.

For reads, `ErrIndeterminate` just means "no answer"; a read has no outcome to be unsure about.

**Why `Unavailable` for `ErrFailed`**, not `FailedPrecondition`: from the client's point of view this node can't
serve writes, and from Phase 3 on another node can. `Unavailable` is the code clients treat as "try elsewhere".

## Server (`internal/server`)

- Depends on a small interface defined where it's used, not on `localdb`:

  ```go
  type Store interface {
      Get(key []byte) ([]byte, error)
      Put(ctx context.Context, key, value []byte) error
      Delete(ctx context.Context, key []byte) error
  }
  ```

  `*localdb.DB` satisfies it today; Phase 3's shard will.
- **The request `ctx` is passed straight to the store**, so a client deadline is honored while the write waits for
  the writer slot, and not after its WAL append starts (Phase 1 rule).
- **Message size limit**: the server's `MaxRecvMsgSize` is set to `MaxKeySize + MaxValueSize + 4 KiB` instead of
  gRPC's default 4 MiB, so an oversized request is rejected before it is buffered in memory. The store still
  validates exactly; the gRPC limit only caps memory.
- **Data-model errors move to `internal/store`.** `ErrNotFound`, `ErrInvalidArgument` and the size limits describe
  the data model, which outlives `localdb` (deleted in Phase 3). `localdb` keeps its names as aliases
  (`var ErrNotFound = store.ErrNotFound`), so nothing in Phase 1 changes. Rejected alternative: the server
  imports `localdb` errors, which would have to be rewritten in Phase 3.
- No extra locking: gRPC runs each RPC on its own goroutine, and `localdb` is already safe for concurrent use.

## Client (`pkg/client`)

```go
func New(addr string) (*Client, error)                     // grpc.NewClient: connects lazily
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error)   // ErrNotFound
func (c *Client) Put(ctx context.Context, key, value []byte) error
func (c *Client) Delete(ctx context.Context, key []byte) error
func (c *Client) Close() error
```

- Imports only `api/kvpb` and gRPC, never `internal/`, so it is usable from outside the module.
- One attempt per call, no retries (Phase 6). The caller's `ctx` sets the deadline.
- Fails fast when the server is down (gRPC's default: no wait-for-ready), returning `ErrIndeterminate`.

## Node binary (`cmd/kvnode`)

```
kvnode -dir DIR [-addr 127.0.0.1:7070] [-snapshot-interval 5m]
```

**Startup:** open the DB (runs recovery) → listen → serve. The port only opens after recovery, so no client can
read a partially recovered state. A recovery error exits non-zero; we never serve from a store we couldn't
recover. The bound address is logged, so tests can pass `-addr 127.0.0.1:0` and read the real port.

**Shutdown** on SIGINT/SIGTERM, in this order:
1. `grpcServer.GracefulStop()`: stop accepting, let in-flight RPCs finish. After a timeout (10 s), `Stop()` cancels
   the rest. A cancelled write still finishes its WAL append, so this never tears a record.
2. Stop the snapshot ticker and wait for its goroutine.
3. `db.Close()`.

The order matters: closing the DB first would turn in-flight writes into `ErrClosed`.

**Snapshot ticker.** Phase 1 left triggering to the caller, and Raft takes it over in Phase 3. Until then, a
long-running node needs *something*, or the WAL and recovery time grow without bound. A goroutine owned by `main`
calls `db.Snapshot` every `-snapshot-interval`. It's cheap when nothing changed (Phase 1 made that a no-op), and a
failed snapshot is logged and retried at the next tick. Rejected alternative: trigger by WAL size. It's the better
policy, but it needs new plumbing in `localdb`, which is deleted in Phase 3.

## Toolchain and dependencies (need your OK)

| Dependency | Why |
|---|---|
| `google.golang.org/grpc` | The RPC framework (settled: gRPC). Includes `test/bufconn` for in-memory tests. |
| `google.golang.org/protobuf` | Generated message code and runtime. |
| `github.com/bufbuild/buf` (tool only) | Runs code generation and proto linting. |
| `google.golang.org/protobuf/cmd/protoc-gen-go`, `google.golang.org/grpc/cmd/protoc-gen-go-grpc` (tools only) | Code generators. |

- **Tools are pinned in `go.mod` with `tool` directives** (Go 1.24+) and run with `go tool buf generate`. Nobody
  installs `protoc` by hand, and CI uses exactly the versions in `go.mod`. Rejected alternative: `protoc` installed
  per machine. Versions drift, and generated code then differs between machines.
  The cost: `buf` has a large dependency tree, and tool dependencies are listed in the main `go.mod`/`go.sum`
  (as indirect requirements; they are never linked into our binaries). If that noise bothers you, the
  alternative is a separate `tools/go.mod` module just for tools.
- **Generated code is checked in**, so `go build` works without the toolchain. CI regenerates and fails if the
  result differs from what's committed (`git diff --exit-code`).
- Versions: latest stable at step 0, pinned in `go.mod`/`go.sum`.

## Test plan

| Area | Test |
|---|---|
| Error mapping | Table test with a fake `Store` returning each error: right code on the wire, right client error, and `ErrIndeterminate` exactly where the table says |
| Deadline propagation | Fake store blocks until its `ctx` ends: the client's deadline reaches the server-side `ctx` |
| Size limit | Oversized request → `ResourceExhausted` → `client.ErrInvalidArgument`; limits exactly → accepted |
| Client + server + real DB | Over `bufconn` (in memory, no ports): random ops against a map model, seed logged |
| Empty value vs missing | `Put(k, "")` then `Get` returns an empty, non-nil value, not `ErrNotFound` |
| Binary: lifecycle | Build `kvnode`, start on port 0, write through the client, SIGTERM → exit 0, restart → data present |
| Binary: crash | Client writes while the parent `SIGKILL`s the node; restart; every acknowledged write present, state is a prefix of the issued ops (Phase 1's check, now across the network) |
| Binary: bad start | Unrecoverable data dir → non-zero exit, port never opened |

No `time.Sleep`: tests wait for the "listening" log line and for process exit.

## Implementation steps (one reviewable change each)

0. Boilerplate: dependencies and tool directives, `buf.yaml`/`buf.gen.yaml`, `api/kvpb/kv.proto` + generated code,
   `make proto`, CI freshness check and `buf lint`.
1. Move data-model errors and limits to `internal/store` (`localdb` aliases them).
2. `internal/server`: handlers, error mapping, size limit; tests with a fake store over `bufconn`.
3. `pkg/client`: client and error classification; client + server + real DB tests over `bufconn`.
4. `cmd/kvnode`: flags, startup, graceful shutdown, snapshot ticker; binary tests (SIGTERM, SIGKILL, bad start).

## Resolved decisions (2026-11-18)

1. Dependencies approved: `google.golang.org/grpc`, `google.golang.org/protobuf`; `buf`, `protoc-gen-go` and
   `protoc-gen-go-grpc` as tools pinned in `go.mod`.
2. A missing key is `found = false` with status `OK`; the client maps it to `ErrNotFound`.
3. `client.ErrIndeterminate` wraps every error that doesn't prove a write was or wasn't applied.
4. `kvnode` runs a snapshot ticker (`-snapshot-interval`, default 5m) until Raft takes over in Phase 3.
5. No `cmd/kvctl` for now.
