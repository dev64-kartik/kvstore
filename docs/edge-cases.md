# Edge cases

Every failure scenario we've identified, how it's handled, and which test covers it.
Status: **planned** (designed, not yet built) · **handled** (code + test exist) · **known gap** (deliberately not handled).

## Phase 1 — single-node durability

| # | Scenario | Handling | Where | Test | Status |
|---|---|---|---|---|---|
| 1.1 | Crash mid-append: last record partially on disk (short header/payload) | Torn tail: truncate at last good offset, fsync | `internal/wal` reader | — | planned (step 5) |
| 1.2 | Crash after the file size is persisted but before the data: zero/garbage-filled tail | Bad CRC, no valid record after it → torn tail, truncate | `internal/wal` reader | — | planned (step 5) |
| 1.3 | Bit flip in a record mid-log | Valid record found after it → `ErrCorrupt`, never skipped | `internal/wal` reader | — | planned (step 5) |
| 1.4 | Bit flip in a *length* field near EOF, making real damage look like a torn tail | CRC covers length; forward scan finds later valid record → `ErrCorrupt` | `internal/wal` reader | — | planned (step 5) |
| 1.5 | Corrupt length claims a huge payload | Reject `length > MaxPayload` before allocating | `internal/wal` codec | — | planned (step 2) |
| 1.6 | Seq gap or regression (missing/reordered segment) | `ErrCorrupt` | `internal/wal` reader, `internal/localdb` recovery | — | planned (steps 5, 7) |
| 1.7 | Bad record in a sealed (non-last) segment | Always `ErrCorrupt` | `internal/wal` reader | — | planned (step 5) |
| 1.8 | Crash while creating a segment | Created as `.tmp` + header + fsync + rename + dir fsync; `.tmp` deleted on recovery | `internal/wal` writer | — | planned (step 4) |
| 1.9 | New file's directory entry not persisted | Dir fsync after create/rename; crashFS undoes un-synced dir entries | `internal/vfs`, writers | — | planned (steps 3, 4) |
| 1.10 | `Write`/`Sync` error on WAL | Failed state: writes return `ErrFailed`, reads continue | `internal/localdb` | — | planned (step 7) |
| 1.11 | Failed fsync leaves never-persisted bytes in page cache; same-OS reopen sees them | Not fully handled: shows up as `ErrCorrupt` after reboot; Phase 3 rebuilds from peers | — | — | known gap |
| 1.12 | Crash during snapshot before rename | `.tmp` deleted; old snapshot + all segments replayed | `internal/localdb` recovery | — | planned (step 7) |
| 1.13 | Crash after snapshot rename, before old segments are deleted | Replay skips `seq ≤ snapSeq` | `internal/localdb` recovery | — | planned (step 7) |
| 1.14 | Snapshot file corrupt (bit rot / bug) | `ErrCorrupt`, no silent fallback | `internal/store` snapshot decode | — | planned (step 6) |
| 1.15 | Caller mutates key/value buffer after `Put` / mutates value returned by `Get` | Store copies in and out | `store.Map.Put` / `Get` | `TestMapCopiesInAndOut` | handled |
| 1.18 | Write to the live map leaks into a snapshot clone (in-place update of shared storage) | `Put` always inserts fresh slices; btree copies shared nodes before writing | `store.Map.Put`, `google/btree` COW | `TestCloneIsolatedUnderConcurrentWrites`, clone checks in `TestMapMatchesModel` | handled |
| 1.19 | Empty value confused with missing key | A found value is always non-nil | `store.Map.Get`, `copyBytes` | `TestMapEmptyValueIsNotMissing` | handled |
| 1.16 | `ctx` cancelled while waiting for the writer | Return `ctx.Err()`, nothing written | `internal/localdb` | — | planned (step 7) |
| 1.17 | Two processes open the same data dir | Not handled (no `flock`) | — | — | known gap |
