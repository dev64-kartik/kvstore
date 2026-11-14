# Phase 1 design — single-node durable KV

Status: **APPROVED 2026-11-14**

## Package layout

| Package | Contents | Fate in Phase 3 |
|---|---|---|
| `internal/store` | Sorted map (`google/btree`) + snapshot format | Kept: becomes the FSM's state |
| `internal/wal` | Record codec, segment writer/reader, tail repair | Deleted (the Raft log replaces it) |
| `internal/vfs` | `FS` interface, `osFS`, test-only `crashFS` | Kept if useful for snapshot I/O, else deleted |
| `internal/localdb` | `DB`: ties WAL + map + snapshots together, recovery | Deleted (replaced by `internal/fsm`) |

## Goals

- A Go API (no network) for a single-node key-value store: `Get`, `Put`, `Delete`, `Snapshot`, `Close`.
- State lives in an in-memory sorted map. Durability comes from a write-ahead log (WAL) plus periodic snapshots.
- `Put`/`Delete` return `nil` only after the WAL record is durable (fsync).
- Crash recovery: the latest snapshot plus a WAL replay gives back every acknowledged write and nothing that was
  never written.
- Recovery truncates a torn record at the **tail** of the log. Corruption **anywhere else** is a hard error.
- Crash tests that model power loss (dropped unsynced data), not just process death.

## Non-goals (deliberately deferred)

| Not doing | Why |
|---|---|
| Group commit / batching | The WAL is retired in Phase 3. Optimizing it is wasted work. One fsync per write. |
| CAS | Comes with Raft in Phase 3, as its own log entry type. |
| Range scans in the API | Not in scope (single-key ops only). Sorted order still matters for deterministic snapshots and future range sharding. |
| Automatic snapshot triggers | Policy, not mechanism. Phase 1 exposes `Snapshot()`; Raft will drive it in Phase 3. |
| File preallocation, `O_DIRECT`, compression | Performance work with no bearing on correctness. |
| Multi-process protection (`flock` on the data dir) | Single test process. Listed as a known gap in edge cases. |
| Falling back to an older snapshot | We keep exactly one. See "Snapshot corruption" below. |

## Data model and API

```go
// package localdb
func Open(dir string, opts Options) (*DB, error)          // runs recovery
func (db *DB) Get(key []byte) ([]byte, error)             // ErrNotFound
func (db *DB) Put(ctx context.Context, key, value []byte) error
func (db *DB) Delete(ctx context.Context, key []byte) error
func (db *DB) Snapshot(ctx context.Context) error
func (db *DB) Close() error
```

- Keys: 1 byte to 4 KiB. Values: 0 bytes to 1 MiB. Anything outside → `ErrInvalidArgument`. The limits bound the
  max record size, and the reader depends on that bound (see below).
- The store **copies** keys and values on the way in and out. Callers may reuse their buffers.
- `Delete` of a missing key succeeds. It is still logged, which keeps the write path uniform.
- `ctx`: checked while waiting for the writer slot. **Once a WAL write has started it is not cancellable**: abandoning
  a half-written record mid-fsync would leave us unsure what's on disk. `Get` does no I/O, so it takes no `ctx`.

## Concurrency

Two locks. **Lock order: `writeSlot` before `mu`, never the reverse.** Readers never take `writeSlot`.

| Lock | Guards | Held by |
|---|---|---|
| `writeSlot` (1-slot `chan struct{}`) | The whole write path: seq assignment, encode, append, fsync, apply | `Put`, `Delete`; `Snapshot` only for rotation + clone |
| `mu sync.RWMutex` | The in-memory tree | `Get` (read lock); `Put`/`Delete` for the in-memory update only; `Snapshot` for the O(1) `Clone()` |

```
Put:  acquire writeSlot (honors ctx) → seq++ → encode → write → fsync → mu.Lock → tree.Set → mu.Unlock → release → return nil
Get:  mu.RLock → lookup → copy value → mu.RUnlock → return
```

- `writeSlot` is a channel, not a `sync.Mutex`, because `Mutex.Lock()` can't be abandoned when `ctx` is cancelled.
- Writes are fully serialized, so the tree is updated in log (seq) order. Throughput is capped at roughly
  1/fsync-latency. That's accepted, because the WAL is retired in Phase 3.
- **Visibility / linearizability.** `Put` returns only after the tree update. A `Get` that starts after `Put` returns
  always sees it. A `Get` that overlaps the `Put` may see either value, which is correct for concurrent operations.
  The linearization point is the tree update under `mu`.
- **Why fsync comes before the tree update.** In the other order, a reader could see a value that a crash then
  erases. That's a read of data that, according to the log, never existed.

## WAL format

All integers little-endian. Checksum: CRC32C (Castagnoli). It's hardware-accelerated and in the stdlib
(`hash/crc32`).

**Segment file** `wal-<startSeq, 20 digits>.log`:

```
header (16 bytes):  magic u32 = 0x4C57564B ("KVWL") | version u16 = 1 | reserved u16 = 0 | startSeq u64
records...
```

New segments are created as `*.tmp`, the header is written, then file fsync → rename → dir fsync. So a segment
either doesn't exist or has a complete header, and a bad header is always a hard error, never a torn write.

**Record**:

```
length u32   // payload length
crc    u32   // CRC32C over (length bytes || seq bytes || payload)
seq    u64   // strictly +1 from the previous record, across segments
payload      // op u8 (1=Put, 2=Delete) | keyLen uvarint | key | [valLen uvarint | value]
```

- The CRC covers `length`, so a bit-flipped length fails the check and isn't trusted.
- The reader rejects `length > MaxPayload` *before allocating*. Otherwise a corrupt length would mean a 4 GiB
  `make([]byte, n)`.
- Sequence numbers start at 1. A gap or regression is corruption.

## Torn tail vs corruption (the subtle part)

Invariant: each append is fsynced before the next begins. So at a crash, **at most the final record** can be
partially persisted.

Reading the **last** segment, at offset `X` we hit a bad record. Causes: short header, length out of range, short
payload, or CRC mismatch. Classify it:

1. Scan forward from `X+1` to EOF for any offset that parses as a valid record (length in range, `seq` equal to
   `lastGoodSeq+1` or `+2`, CRC matches). The seq check is cheap and filters almost every offset before any CRC is
   computed.
2. **No valid record after `X`** → torn tail. Truncate the file to `X`, fsync, log a warning with the byte count,
   and continue appending at `X`.
3. **A valid record after `X`** → acknowledged data sits behind the damage. Return `ErrCorrupt` (offset, segment,
   last good seq). Never skip it.

A bad record in any **non-last** segment is always `ErrCorrupt`. Those segments were sealed.

Alternatives rejected:
- *Truncate at the first bad record, always* (LevelDB's default recovery mode). It silently drops acknowledged writes
  after a mid-log bit flip. That violates the core rule.
- *Only a short read counts as torn; a CRC mismatch is always corruption.* It fails on real crashes. The filesystem
  can persist the file size before the data, which gives a full-length record of zeros or garbage at the tail.
- *Bound the damage to "within MaxPayload of EOF".* Simpler, but it still truncates real records when a length gets
  corrupted near the end. The forward scan closes that gap.

## Write failure policy

If any WAL `Write` or `Sync` returns an error, the DB enters a **failed** state:
- Every later `Put`/`Delete`/`Snapshot` returns `ErrFailed`.
- `Get` keeps working. The tree only ever contains acknowledged writes, so reads are still correct.
- The failed write was never acknowledged, so after any recovery it may or may not be present. Both are allowed.

Why not keep writing:
- **Partial write:** part of a record may be in the file. Appending after it produces garbage followed by valid
  records, which our own recovery rule classifies as mid-log corruption.
- **Failed fsync:** on Linux, writeback failure marks the dirty pages *clean* and reports the error once. A retried
  fsync finds nothing dirty and returns success, even though the data never reached disk (PostgreSQL "fsyncgate",
  2018).

**Known limitation.** Closing and re-opening the DB in the same OS instance does **not** guarantee a correct view.
The page cache can still serve the never-persisted bytes, so recovery may accept them and later appends land after
them. After a reboot this shows up as `ErrCorrupt` at `Open`: loud, never silently wrong. Fail-stop guarantees no
acknowledgement after a durability failure; it does not guarantee automatic local recovery. On a single node
(Phases 1–2) an operator handles it. From Phase 3, a node with a failed disk is wiped and rebuilt from its Raft
peers.

## Snapshots

**File** `snap-<seq, 20 digits>.snap`:

```
magic u32 | version u16 | reserved u16 | seq u64 | count u64 | entries (keyLen uvarint, key, valLen uvarint, value)... | crc u32 (over everything before it)
```

Entries are written in key order, so the same state always produces the same bytes. That makes it easy to test
and lines up with Raft snapshots later.

**Procedure** (`Snapshot()`; concurrent calls are serialized):
1. Holding `writeSlot`: rotate the WAL to a new segment starting at `lastSeq+1`. Then take `mu` just for
   `tree.Clone()` (O(1)). `google/btree` forbids running `Clone` concurrently with mutations, and taking `mu` also
   keeps it clean under `-race`. Record `snapSeq = lastSeq`, then release `writeSlot`. Writes continue into the new
   segment.
2. Without holding any lock: write `snap-<snapSeq>.snap.tmp` → fsync → rename → fsync dir.
3. Delete all segments before the new one and all older snapshots, then fsync dir.

**Crash points**, all safe because recovery filters by seq:
- Before step 2's rename: `.tmp` left over. Recovery deletes `*.tmp` and replays from the old snapshot plus all
  segments.
- After rename, before step 3: new snapshot plus old segments. Recovery skips records with `seq ≤ snapSeq`.
- Halfway through step 3: some old segments are gone. Same as above.

**Snapshot corruption**: the rename is atomic, so a bad snapshot means bit rot or a bug, not a crash. That's
`ErrCorrupt`, not a silent fallback. The old WAL is already deleted, so falling back would lose data without
anyone noticing.

## Recovery (`Open`)

1. List the dir, delete `*.tmp`, and fsync the dir.
2. Load the highest `snap-*.snap` (verify the CRC) or start empty with `snapSeq = 0`.
3. Open segments in `startSeq` order. Each header's `startSeq` must equal the previous segment's last seq + 1.
4. Replay records with `seq > snapSeq`. The first replayed seq must be `snapSeq + 1`, or else `ErrCorrupt`
   (a missing segment).
5. Tail-repair the last segment as described above, then open it for appending. If no segment exists, create one.

## Sorted map

Decision: `github.com/google/btree` (the generic `BTreeG`). Its `Clone()` is lazy copy-on-write in O(1):
- After a clone, both trees share every node. Each node records which tree owns it, and a tree mutates a node in
  place only if it owns that node.
- On the live tree's next write, it copies the root-to-leaf path it touches (O(log n) nodes) and mutates the copies.
  The clone keeps pointing at the old nodes, a frozen point-in-time view that the snapshot writer walks without
  locks.
- Cost: O(log n) extra copying per write after a clone. While a slow snapshot runs, memory can reach up to 2× in
  the worst case.
- Why not OS-level COW via `fork()` (as Redis `BGSAVE` does): the Go runtime is multi-threaded, and `fork` without
  `exec` is unsafe there.

| Alternative | Rejected because |
|---|---|
| Hand-written skiplist | Weeks of work on something that isn't the focus. A snapshot needs either a full copy under lock or our own COW. |
| `map` + sort at snapshot time | O(n log n) snapshot. And "sorted map" is a settled decision. |
| Sorted slice | O(n) inserts. |

## Filesystem abstraction (for crash testing)

`SIGKILL` doesn't simulate power loss, because the OS page cache survives process death. So a missing fsync is
invisible to a kill test. Add a small `FS` interface (open, create, rename, remove, list, sync-dir; file: write,
sync, truncate, close) with two implementations:

- `osFS`: thin wrapper over `os`. Note: on macOS, Go's `File.Sync` issues `F_FULLFSYNC`, which is correct and slow.
- `crashFS` (test-only): an in-memory FS that tracks synced and unsynced state per file **and per directory entry**.
  `Crash()` drops unsynced bytes, optionally keeping a random prefix (a torn write). It also undoes creates and
  renames that were never dir-fsynced. This is what catches a missing dir fsync.

## Test plan

| Area | Test |
|---|---|
| Sorted map | Model-based: random ops vs `map[string][]byte`, seed logged and replayable |
| Record codec | Round-trip table tests, plus `FuzzDecode` (native Go fuzzing: never panics, never over-allocates) |
| Reader | One table case per failure: short header, short payload, CRC mismatch at tail, zero-filled tail, oversized length, corrupt record mid-file (→ error), corrupt *length* near EOF with valid records after (→ error via forward scan), seq gap, bad segment header |
| Snapshot | Round-trip; deterministic bytes; flipped byte → `ErrCorrupt` |
| Crash (crashFS) | Random ops, crash at a random FS call, recover. Every acknowledged write is present. Each key's value is either its last acknowledged value or a value from an unacknowledged op issued after it. Seeded. |
| Crash (process) | Re-exec the test binary as a child that writes and reports acks over a pipe. The parent `SIGKILL`s it at a random moment, recovers, and checks acks. |
| Fault injection | `Sync` returns an error → `ErrFailed` on all later writes; reopen recovers. |

## Implementation steps (one reviewable change each, each ≤ ~300 lines incl. tests)

0. Boilerplate: `go.mod`, Makefile, `.golangci.yml`, GitHub Actions CI, `.gitignore`.
1. Sorted map wrapper + model-based test.
2. WAL record encode/decode + fuzz test.
3. FS interface + `osFS` + `crashFS`, with `crashFS` tests.
4. WAL segment writer: header, append+fsync, failed-state policy.
5. WAL reader: replay, seq checks, torn-tail classification and truncation.
6. Snapshot encode/decode + atomic install.
7. `DB`: `Open`/recovery, write path, `Snapshot` with rotation and GC.
8. Crash tests: randomized crashFS + `SIGKILL` subprocess test.

## Resolved decisions (2026-11-14)

1. `github.com/google/btree` approved as a dependency.
2. Package layout: `store` / `wal` / `vfs` / `localdb`, as in the table at the top.
3. `ctx` is taken on `Put`/`Delete`/`Snapshot` now. It's honored while waiting for `writeSlot` and not after a WAL
   write starts.
