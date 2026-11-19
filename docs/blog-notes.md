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

## My crash simulator was kinder than the disk (2026-11-17)

Step 8 measured the new randomized crash test by re-introducing old bugs. It caught a missing WAL fsync, a
missing torn-tail truncation and a missing snapshot fsync. It did *not* catch a missing directory fsync after
the snapshot rename. The reason: crashfs keeps a *prefix* of unsynced directory operations, so the cleanup's
later `remove`s can only survive if the `rename` before them did. And the cleanup ends with its own directory
fsync, which persists the rename anyway. Real filesystems promise no ordering between unsynced directory
operations, so a disk could persist the removals of the old WAL segments and lose the new snapshot's name:
total data loss. The targeted test (crash at every op of `WriteSnapshot`) still catches it, because it crashes in
the window right after the rename. Lessons: a simulator encodes assumptions, and "it passes under simulation" is
only as strong as the simulator's weakest one. Mutation testing (break it on purpose, see who notices) is how I
found out which assumption it was. The same run showed the `SIGKILL` test can't catch a missing fsync at all,
since the OS keeps the page cache across a process crash, so it has to be paired with power-loss simulation.

## A test-helper flake that was hiding a production bug (2026-11-18)

CI on Linux failed a client test with "Serve: grpc: the server has been stopped". It never failed on my Mac, even
in 300 repeated runs. The test stopped a gRPC server right after starting it, and the helper started `Serve` in a
goroutine. On a fast runner, `Stop` sometimes ran before that goroutine reached `Serve`, and `Serve` then returns
`ErrServerStopped`. The tempting conclusion was "flaky test, ignore that error in the helper". But the node binary
has the same shape: `go g.Serve(lis)`, then shutdown on SIGTERM. A SIGTERM right after startup would make `run()`
report a serving error and exit 1 on a perfectly clean shutdown. Two lessons. A flake is a race somewhere, and the
first question is whether production code has the same shape. And when a race won't reproduce by repetition,
force the interleaving: stop *before* serving (deterministic), or start from an already-cancelled context (which
loses the race almost every time).
