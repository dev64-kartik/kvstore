# Blog notes

Raw material for the blog series: bugs, surprises and tradeoffs, written down when they happen.

## 2026-11-15 — Checksumming the length field buys almost nothing (step 2)

The design said the record CRC covers the length field "so a corrupted length isn't trusted". To test that, I removed
the length from the CRC, and no test failed. The reason: a corrupted length already changes *which bytes* get
checksummed, so the CRC fails anyway. LevelDB and RocksDB don't cover it either. We kept it because it's free, but
the design doc's claim was wrong. Lesson: deliberately break the code to find out what a design decision actually
buys.

## 2026-11-15 — A test for the crash simulator itself (step 3b)

crashFS models "durable" and "current" bytes per file. I broke `Sync` on purpose, making the durable bytes share
memory with the live buffer, and every test still passed. Appends never write inside the synced range, so the
sharing was invisible. It only shows when you truncate and then append over the cut, which is exactly what WAL
torn-tail repair does. A new test now catches it (`"abcXef"` instead of `"abcdef"`). Lesson: the fault injector
needs its own adversarial tests, or a broken simulator quietly approves broken storage code.

Related: Go randomizes map iteration order, so a seeded crash simulator that loops over a map isn't replayable.
`TestCrashIsDeterministic` catches that.

## A snapshot bug that today's ops hide (2026-11-17)

A snapshot claims "this is the state at seq N". If the clone were taken a moment after reading N, it could also
hold writes N+1, N+2. Recovery would then replay N+1.. on top of a state that already contains them. With only
Put and Delete nothing goes wrong: each replayed op overwrites the key with the same result, so the final state
is identical, and no end-state test can see the bug. Put and Delete are idempotent *as a sequence suffix*.
Compare-and-swap isn't: replaying "CAS k from 1 to 2" on a state where k is already 2 fails, so the outcome
differs. The rule (seq and clone in one critical section) has to be right before Phase 3 makes it observable.
Lesson: a correctness test that only checks the final state is blind to bugs that the operations' algebra
masks.
