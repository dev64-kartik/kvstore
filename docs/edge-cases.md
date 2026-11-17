# Edge cases

Every failure scenario we've identified, how it's handled, and which test covers it.
Status: **planned** (designed, not yet built) · **handled** (code + test exist) · **known gap** (deliberately not handled).

## Phase 1 — single-node durability

| # | Scenario | Handling | Where | Test | Status |
|---|---|---|---|---|---|
| 1.1 | Crash mid-append: last record partially on disk (short header/payload) | Torn tail: scan stops at the last good offset; `Open` truncates there and syncs before any append | `wal.scanSegment`, `wal.openLast` | `TestScanTornTailAtEveryCut`, `TestOpenRepairsTornTail`, `TestCrashRecoverAppendCycles` | handled |
| 1.2 | Crash after the file size is persisted but before the data: zero/garbage-filled tail, or a full-length record with a zeroed sector | Bad record with no valid record after it → torn | `wal.scanSegment` | `TestScanTornTailShapes` | handled |
| 1.3 | Bit flip in a record mid-log | Forward scan finds the next valid record → `ErrCorrupt`, never skipped | `wal.scanSegment`, `findLaterRecord` | `TestScanMidLogDamageIsCorrupt` | handled |
| 1.4 | Bit flip in a *length* field mid-log, making real damage look like a torn tail | Forward scan finds the next valid record → `ErrCorrupt` | `wal.findLaterRecord` | `TestScanMidLogDamageIsCorrupt` ("length points past EOF") | handled |
| 1.5 | Corrupt length claims a huge payload | `length > MaxPayload` → `ErrBadRecord` (not `ErrTruncated`), checked before the length is used | `wal.DecodeRecord` | `TestDecodeOversizedLengthIsBadNotTruncated` | handled |
| 1.20 | Single bit flip anywhere in a record | CRC32C (guaranteed for crc/seq/payload; length flips change the checksummed range) | `wal.DecodeRecord` | `TestDecodeDetectsEveryBitFlip` | handled |
| 1.21 | Record cut short at any byte | `ErrTruncated` | `wal.DecodeRecord` | `TestDecodeTruncatedAtEveryLength` | handled |
| 1.22 | Valid CRC but malformed payload (writer bug) | Payload parsed strictly → `ErrBadRecord` | `wal.decodePayload` | `TestDecodeMalformedPayloadWithValidChecksum` | handled |
| 1.23 | Record that can't be read back gets written | Same limits checked on encode → `ErrInvalidRecord` | `wal.AppendRecord` | `TestAppendRecordRejectsInvalid`, `FuzzRecordRoundTrip` | handled |
| 1.25 | Failed open returns a non-nil `File` wrapping a nil `*os.File`, so `f != nil` checks pass and `Close` fails | `wrap` returns a literal nil on error | `vfs.wrap` | `vfstest` `MissingFileIsNotExist` | handled |
| 1.24 | Appending to a decoded key overwrites the next field | Decoded slices have capacity capped (`p[:n:n]`) | `wal.readLengthPrefixed` | `TestDecodeAliasesInput` | handled |
| 1.6 | Seq gap or regression (missing/reordered segment) | In a segment: checksum-valid record with the wrong seq → `ErrCorrupt`. Across segments: each must start where the previous ended → `ErrCorrupt` | `wal.scanSegment`, `wal.Open` | `TestScanSeqOutOfOrderIsCorrupt`, `TestOpenRejectsBrokenLayouts` | handled |
| 1.7 | Bad record in a sealed (non-last) segment | Always `ErrCorrupt` | `wal.scanSegment` | `TestScanTornTailAtEveryCut` (sealed case) | handled |
| 1.8 | Crash while creating a segment | `.tmp` + header + fsync + rename + dir fsync, so a visible name always has a complete header; `Open` removes leftover segment `.tmp` files (otherwise `O_EXCL` would block that segment forever) | `wal.CreateSegment`, `wal.removeTempSegments` | `TestCreateSegmentCrashAtEveryOp`, `TestOpenRemovesTempSegmentsAndIgnoresStrangers` | handled |
| 1.9 | New file's directory entry not persisted | Dir fsync after rename, before any record is acknowledged into the segment | `crashfs.FS.Crash` (simulation), `wal.CreateSegment` | `TestSyncedFileWithoutDirSyncVanishes`, `TestRenameAndRemoveNeedDirSync`, `TestCreateSegmentCrashAtEveryOp` | handled for segments; snapshots: step 6 |
| 1.26 | Crash simulator itself wrong: synced bytes changed by later unsynced writes (truncate then append) | `Sync` stores a separate copy | `crashfs` `file.Sync` | `TestDropUnsyncedRevertsTruncateAndOverwrite` | handled |
| 1.27 | Crash simulator not replayable: same seed, different outcome (Go map iteration order is random) | `Crash` walks directories and entries in sorted order | `crashfs.FS.Crash` | `TestCrashIsDeterministic` | handled |
| 1.10 | `Write`/`Sync` error on WAL | Writer permanently failed (`ErrFailed`), no retry; DB-level: writes refused, reads continue | `wal.Writer.fail`; DB: `internal/localdb` | `TestIOErrorFailsWriterForever` | handled in WAL; DB: step 7 |
| 1.11 | Failed fsync leaves never-persisted bytes in page cache; same-OS reopen sees them | Not fully handled: shows up as `ErrCorrupt` after reboot; Phase 3 rebuilds from peers | — | — | known gap |
| 1.12 | Crash during snapshot before rename | Data synced before rename; leftover `.tmp` removed on load; the previous snapshot (and the WAL, step 7) is still used | `store.WriteSnapshot`, `store.LoadLatest` | `TestWriteSnapshotCrashAtEveryOp` (every op × both policies) | handled in store; WAL side: step 7 |
| 1.13 | Crash after snapshot rename, before old segments are deleted | Replay skips `seq ≤ snapSeq` | `internal/localdb` recovery | — | planned (step 7) |
| 1.14 | Snapshot file corrupt (bit rot / bug) | CRC checked before any field is trusted, then strict parsing (key order, count, trailing bytes); no fallback | `store.decodeSnapshot` | `TestSnapshotDetectsEveryBitFlipAndTruncation`, `TestSnapshotMalformedWithValidChecksum`, `FuzzDecodeSnapshot` | handled |
| 1.15 | Caller mutates key/value buffer after `Put` / mutates value returned by `Get` | Store copies in and out | `store.Map.Put` / `Get` | `TestMapCopiesInAndOut` | handled |
| 1.18 | Write to the live map leaks into a snapshot clone (in-place update of shared storage) | `Put` always inserts fresh slices; btree copies shared nodes before writing | `store.Map.Put`, `google/btree` COW | `TestCloneIsolatedUnderConcurrentWrites`, clone checks in `TestMapMatchesModel` | handled |
| 1.19 | Empty value confused with missing key | A found value is always non-nil | `store.Map.Get`, `copyBytes` | `TestMapEmptyValueIsNotMissing` | handled |
| 1.16 | `ctx` cancelled while waiting for the writer | Return `ctx.Err()`, nothing written | `internal/localdb` | — | planned (step 7) |
| 1.17 | Two processes open the same data dir | Not handled (no `flock`) | — | — | known gap |
| 1.28 | New segment created over a live one (stale startSeq), silently replacing acknowledged data via rename | Refuse if the name exists → `fs.ErrExist` | `wal.CreateSegment` | `TestCreateSegmentRefusesExisting` | handled |
| 1.29 | Acknowledged WAL record lost after power loss | One `Write` + `Sync` per record; ack only after `Sync` | `wal.Writer.Append` | `TestAckedAppendsSurviveCrash`, `TestCrashDuringAppendsKeepsAckedPrefix` | handled |
| 1.30 | Rejected (invalid) record poisons the writer or uses up a seq | Validation before any I/O; seq advanced only after `Sync` | `wal.Writer.Append` | `TestInvalidRecordDoesNotFailWriter` | handled |
| 1.31 | Torn in-flight record whose *value* contains a forged, checksum-valid record with the next seq: the forward scan finds it and reports corruption | Not handled: the DB refuses to start (loud, no data lost). The fix would be to skip over the bad record's declared length when it's plausible, which would weaken the defense in 1.4 | `wal.findLaterRecord` | — | known gap |
| 1.32 | Bad or short segment header in the last segment "repaired" as a torn write | The header is synced before the file is named, so a bad header is always `ErrCorrupt` | `wal.checkSegmentHeader` | `TestScanBadHeaderIsCorrupt` | handled |
| 1.33 | Log ends before the snapshot, or no segments at all while a snapshot exists | `ErrCorrupt`: appending would reuse seqs the snapshot already covers, or writes after the snapshot are missing | `wal.Open` | `TestOpenRejectsBrokenLayouts` | handled |
| 1.34 | Damage in a segment wholly covered by the snapshot (GC hadn't deleted it yet) | Not read at all, so it doesn't block startup | `wal.Open` | `TestOpenSkipsSegmentsCoveredBySnapshot` | handled |
| 1.35 | Crash during recovery itself (mid-truncate, mid-replay) | Recovery is repeatable: the next `Open` sees the same or an already-repaired tail | `wal.Open` | `TestCrashRecoverAppendCycles` | handled |
| 1.36 | A record recovered once disappears in a later recovery | After a crash everything visible is durable; the cycle test carries each recovery's result into the next | `wal.Open` | `TestCrashRecoverAppendCycles` | handled |
| 1.37 | Snapshot rotation when the current segment is still empty: `CreateSegment(lastSeq+1)` collides with the current segment's own name | Rotation must skip (or the snapshot must be a no-op) when nothing was written since the last rotation | `internal/localdb` | — | planned (step 7) |
| 1.38 | Newest snapshot damaged while an older one is intact | `ErrCorrupt`, no silent fallback: the WAL bridging the older snapshot may already be deleted | `store.LoadLatest` | `TestLoadLatestDoesNotFallBack` | handled |
| 1.39 | Failed or cancelled snapshot leaves a `.tmp` that blocks the next attempt (`O_EXCL`) | Temp file removed on every failure before the rename | `store.WriteSnapshot` | `TestWriteSnapshotFailureCleansUp` | handled |
| 1.40 | Snapshot renamed or misplaced: file name seq ≠ header seq | `ErrCorrupt` | `store.LoadLatest` | `TestLoadLatestRejectsNameHeaderMismatch` | handled |
| 1.41 | Snapshot of a map that writers are still changing | `WriteSnapshot` reads only; the caller passes a `Clone` taken under its lock | `store.WriteSnapshot` contract, `localdb` (step 7) | `TestCloneIsolatedUnderConcurrentWrites` (step 1) | handled by contract; wiring: step 7 |
