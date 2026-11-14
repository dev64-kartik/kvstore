# CLAUDE.md — kvstore

## What this is

A distributed, replicated, sharded key-value store in Go. Data is split across shards; each shard is its own
Raft group (`hashicorp/raft`), replicated across N nodes. Clients use gRPC and get linearizable single-key
reads/writes plus compare-and-swap. Mental model: a small, hand-built TiKV / CockroachDB KV layer.

This is the centerpiece of my portfolio for platform / distributed-systems roles. **Every meaningful line must be
something I can explain and defend in an interview.** Understanding beats speed, always.

## How to work with me (read this first)

I know distributed systems concepts well (I've read DDIA). I'm new to Go and to `hashicorp/raft` (background:
Java, Python, some C++). **You write most of the code; I review every change and learn the system through that
review.** Optimize every change for a human reviewer, not for speed of generation.

**Change size and shape**
- One concept per change. Example sequence: "WAL record encoding", then "WAL reader with CRC check", then
  "torn-tail truncation". Never a whole subsystem in one go.
- Aim for at most ~300 lines of hand-written diff per change (generated code such as protobuf doesn't count). If a
  step needs more, split it and tell me the split first.
- Tests go in the same change as the code they test.
- Don't scaffold ahead: no empty packages, stub files or interfaces for future phases.
- Don't refactor unrelated code in the same change. Ask before adding any dependency.

**Before writing core logic** (WAL, recovery, snapshots, FSM, read path, CAS, sharding, meta-shard, chaos harness):
- Give me a short plan: what this step does, which edge cases it handles, and what it deliberately leaves for later.
  Wait for my OK.
- Boilerplate (Makefile, CI, lint config, proto, gRPC wiring, metrics plumbing) needs no plan; just do it.

**After every change, end with a review note:**
1. What changed and why (a few lines).
2. The key decisions, and the alternatives you rejected.
3. Edge cases: each one, where it's handled (file/function) and which test covers it. Also list the ones not handled yet.
4. Go idioms in this change that a Java developer might misread.
5. Two or three questions that check I've understood it (e.g. "what happens if the process dies between the
   rename and the directory fsync?"). Don't answer them unless I ask.
6. Where useful, one way to break it on purpose (drop a sync, flip a byte) and the test that should catch it.

**Code style for review**
- Comment the *why* on anything non-obvious (an fsync, a lock ordering, a durability assumption). Don't comment
  the obvious.
- Prefer plain, explicit code over clever code. Readability beats brevity.

**Process**
- **Design doc before code.** No implementation for a phase until `docs/phase-N-design.md` exists (goals,
  non-goals, alternatives considered, tradeoffs). You draft it; I edit and approve it.
- Keep `docs/edge-cases.md` current: every failure scenario we've identified, how it's handled, which test covers it.
- Don't commit until I've reviewed. Then commit with a message that explains the why.
- **Push back on shortcuts**: skipped fsyncs, ignored errors, untested recovery paths, stale reads, `time.Sleep`
  in tests, "we'll test it later". Be direct; I prefer honest assessment to reassurance.
- **Don't re-argue settled decisions** (below). If you think one is wrong, say so once with reasoning, then follow it.
- Before saying a change is done: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test -race ./...` all pass.

## Settled decisions

| Decision | Why |
|---|---|
| Go | Ecosystem fit for platform roles; fast iteration |
| `hashicorp/raft`, not from-scratch Raft | Consensus *usage* and multi-Raft are the interesting work; I study the paper and library source instead |
| Multi-Raft: one Raft group per shard | How real systems (TiKV, CockroachDB) scale |
| In-memory sorted map + snapshots, not an LSM tree | Distributed systems are the focus. Storage sits behind a clean interface so Pebble could be swapped in later |
| Phase 1 WAL is retired once Raft lands (Phase 3) | The Raft log becomes the durability log; the store becomes the FSM, persisted via Raft snapshots |
| No MVCC / transactions | Single-key linearizable ops + CAS cover the scope |
| CAS primitive (added in Phase 3) | Lets clients build optimistic concurrency, locks and counters without transactions |
| Linearizable reads via ReadIndex (`VerifyLeader`) | Correct without reasoning about clock skew (leader leases are future work) |
| Internal meta-shard for cluster metadata, not etcd | Forces handling bootstrapping; avoids dependency sprawl |
| Static shard count configured at startup | Range split/merge is a project of its own; metadata is designed to allow it later |

## Architecture

- **Storage** (per node, per shard): sorted in-memory map + snapshots (+ WAL in Phases 1–2 only).
- **Replication** (per shard): a `hashicorp/raft` group. Log entries are KV ops; committed entries are applied
  to the store through the `raft.FSM` interface.
- **Coordination** (cluster-wide): the meta-shard, a Raft group holding membership, shard→node placement and
  Raft group composition.
- **Client API** (gRPC): routes requests to the shard leader using the meta-shard, follows redirects on
  `NotLeader`, caches routing.

## Phases

Each phase leaves a working, demonstrable system.

1. Single-node KV: WAL + in-memory sorted map + snapshots + crash recovery (Go API, no network)
2. gRPC server + simple Go client (single node)
3. Single-shard Raft with 3 nodes, ReadIndex reads, CAS ← heart of the project
4. Multi-Raft + key-hash sharding (resource sharing across groups, batching)
5. Meta-shard: bootstrapping, placement, watches, routing cache invalidation
6. Client routing polish: redirects, retries with backoff, timeouts
7. Observability: Prometheus metrics, `log/slog`, pprof, Grafana dashboard JSON
8. **Chaos harness + Porcupine linearizability checking.** This is the differentiating phase; do not shortcut it.
9. Benchmarks vs etcd, profiling, at least one profile-driven optimization
10. Docs, README, architecture diagram, blog series (ongoing, finished last)

**Current phase:** 1 · **Last thing done:** Phase 1 design approved; step 0 (boilerplate) in review

## Correctness rules

**Durability (Phases 1–2)**
- A write is acknowledged only after its WAL record is durable (fsync, or a documented group-commit policy).
- WAL file: header with magic number + format version. Record: length prefix, CRC32C, sequence number, payload.
- Recovery: a torn or invalid record at the **tail** is truncated. Corruption **before** the tail is an error
  and must never be silently skipped.
- New files: fsync the file **and** its parent directory. Snapshot install: write temp file → fsync → rename →
  fsync directory.
- Never ignore errors from `Write`, `Sync` or `Close` on files.

**Concurrency and consistency (Phase 3+)**
- `hashicorp/raft` serializes `FSM.Apply`. State is guarded by a `sync.RWMutex`: `Apply` takes the write lock,
  reads take the read lock.
- `Apply` must be deterministic: no `time.Now()`, randomness, or map iteration order affecting results.
- Every read on the leader goes through `VerifyLeader()` first. Never serve local state without it.
- CAS is its own log entry type; compare-and-write happens inside `Apply`.
- Never hold a lock across a Raft call, network call or disk I/O.

## Go conventions

- Standard library first. `log/slog` for logging.
- Errors: wrap with `fmt.Errorf("...: %w", err)`; sentinel errors (`ErrNotFound`, `ErrNotLeader`); check with
  `errors.Is`/`errors.As`. No panics for expected failures.
- `context.Context` is the first parameter of anything that blocks or does network I/O.
- Small interfaces, defined where they're consumed. No package-level mutable state, no side effects in `init()`.
- Every goroutine has an owner and a shutdown path. `Close()` must actually stop everything it started.
- Everything lives under `internal/` except `api/` and `pkg/client/`.

Target layout (create directories as phases need them, not up front):

```
cmd/kvnode/        node binary
internal/wal/      Phase 1
internal/store/    sorted map + snapshot format
internal/fsm/      raft.FSM implementation (Phase 3)
internal/shard/    multi-Raft host (Phase 4)
internal/meta/     meta-shard (Phase 5)
internal/server/   gRPC handlers
api/kvpb/          .proto + generated code
pkg/client/        Go client
test/chaos/        Phase 8
bench/             Phase 9
docs/              design docs, blog notes
```

## Commands

```bash
go build ./...
go vet ./...
golangci-lint run
go test -race ./...                        # always with -race
go test -race -run TestName ./internal/wal/...
go test -race -count=50 -run TestName ./...  # shake out flaky/concurrency bugs
```

**CI** (GitHub Actions, standard hosted runners only): build, vet, lint and `test -race` on every push and PR.
From Phase 8, a short, seeded chaos run goes in CI too; long chaos runs stay local. No deployment pipeline: the
cluster runs locally via docker-compose, and cloud VMs are used only temporarily for Phase 9 benchmarks.

## Testing

- Every phase ships with its tests. No "test it later".
- Storage and recovery: property-based tests. Run random ops against the store and against a plain Go map model,
  then compare.
- Crash tests: kill a child process (`SIGKILL`) at random points, or inject faults through a filesystem
  abstraction. After recovery, **every acknowledged write must be present** and nothing that was never written may
  appear. Unacknowledged writes may or may not survive.
- No `time.Sleep` for synchronization. Use channels, or poll with a deadline.
- Randomized tests log their seed and can be replayed from it.
- When a bug is found: write the failing regression test first, then fix it.
- Phase 8: record operation histories and check them with Porcupine; failures must be reproducible from a seed.

## Docs

- `docs/phase-N-design.md`: written before the code, updated when the design changes.
- `docs/edge-cases.md`: failure scenarios → handling → covering test. Doubles as my interview prep.
- When we hit an interesting bug or tradeoff, add a short entry to `docs/blog-notes.md` (raw material for the
  blog series).

## Out of scope (don't build unless I say so)

LSM tree / custom storage engine · from-scratch Raft · MVCC and snapshot isolation · cross-shard transactions ·
dynamic shard split/merge · leader leases · external etcd · client SDKs in other languages.