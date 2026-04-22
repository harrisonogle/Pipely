Three separate reviewers came to a consensus on blocking bugs in the spec. Their summaries of blocking bugs are below.

# Reviewer 1

Three blocking bugs:

**1. First-read loses earlier segments (§7.1 Solution 2)**

The `_head = tail` fallback on first read is only correct when the writer has published exactly once. If the writer publishes A → B → C before the reader's first `ReadAsync`, the reader observes `state.Tail = C`, sets `_head = C`, and has no way to reach A or B — those bytes are lost and their holders' refcounts never drop to zero.

*Fix:* add a `state.Head` field written once by the first `PublishActiveSegment`, read once by the reader on first `ReadAsync`.

**2. `ComputeReadResult()` called from the writer thread (§8.2)**

In `MaybeSignalReaderAwaiter`, the winning writer does `_reader._readAwaiter.SetResult(ComputeReadResult())`. Building the `ReadResult` requires reader-local state (`_head`, `_headConsumedOffset`, `_examinedPosition`, `_lastReturnedBuffer`), and the code then writes `_readInProgress` / `_lastReturnedBuffer` from the writer thread. This violates the SPSC invariant the memory-ordering argument rests on.

*Fix:* `SetResult` with a sentinel (or a minimal struct carrying just `isCanceled`/`isCompleted`); have the reader build the real `ReadResult` from reader-local state when the continuation resumes.

**3. Reader-complete buffer leak with no specified cleanup path (§7.4 / §10.3)**

§7.4 acknowledges that segments/buffers can leak when the writer publishes between the reader completing and the writer observing it. §10.3 claims these are "freed on `SpscPipe` finalization" — but §3's public API defines no `IDisposable` and no finalizer. For `ArrayPool<byte>.Shared` this is pool pressure; for a custom `MemoryPool<byte>` backed by pinned native buffers (which §13 anticipates for AF_XDP) it's a real resource leak.

*Fix:* add `IDisposable` and/or a finalizer that walks any remaining segments and releases their holders and buffers to the pool. Specify the cleanup semantics explicitly.

# Reviewer 2

## Blocking bugs in SpscPipe spec v2 draft

**1. First-read loses earlier segments (§7.1 step 4)**

If the writer publishes multiple segments before the reader's first `ReadAsync`, `state.Tail` points to the most recently published segment. The reader's `_head = tail` assignment skips segments 1…N−1 — their bytes are never delivered, and their refcount increments are never matched, leaking buffers until GC. The spec dismisses the fix (§7.1 Solution 1) with an argument that's incorrect: "since it's the only segment, `tail == firstSegment`" only holds if the reader calls `ReadAsync` before the writer's second publication, which nothing guarantees.

*Fix:* Add a write-once `state.Head` field set by the first publication only. The reader reads it on first `ReadAsync`.

**2. `ComputeReadResult()` called from the writer thread (§8.2)**

In `MaybeSignalReaderAwaiter`, the winning writer does `_reader._readAwaiter.SetResult(ComputeReadResult())`. Building the `ReadResult` requires reader-local state (`_head`, `_headConsumedOffset`, `_examinedPosition`, `_lastReturnedBuffer`, `_readInProgress`), and writing `_readInProgress`/`_lastReturnedBuffer` from the writer thread violates the SPSC single-writer invariant the entire memory-ordering argument rests on. The happens-before edge through `ValueTaskSource`'s continuation delivery is not a general fix and isn't discussed.

*Fix:* `SetResult` with a sentinel or minimal value carrying just `isCanceled`/`isCompleted` flags; build the real `ReadResult` on the reader thread when the continuation resumes. The writer-side flush awaiter doesn't have this problem because `FlushResult` has no reader-local dependencies.

**3. Reader-complete buffer leak via shared holders (§7.4)**

After the reader completes, the writer may publish additional segments sharing a buffer with already-retired segments before observing `ReaderCompletionState`. The reader never retires these new segments, so their refcount increments are never matched, and the buffer stays live with `Refcount ≥ 1` indefinitely. The spec hand-waves this as "freed when `SpscPipe` is GC'd" without specifying a finalizer or `Reset()` path that actually walks the list and releases holders.

*Fix:* Define a finalizer / `Reset()` cleanup that walks segments reachable from `state.Tail` and releases their holders.

---

Everything else from the earlier reviews is spec polish or clarity. Notably, the §7.1 step 3 load order (tail then completion) is **not** a blocking bug — `IsCompleted = true` with a partial buffer is explicitly permitted by the BCL `PipeReader` contract, which documents that consumers must loop until `IsCompleted && Buffer.IsEmpty`. Reordering the two acquire-loads is an editorial improvement (tightens `IsCompleted` timing and lets the tautological `availableEndPosition` check be dropped as dead code), not a correctness fix.

# Reviewer 3

Three blocking bugs:

**1. First-read segment loss.** If the writer publishes multiple segments before the reader's first `ReadAsync`, `state.Tail` points to the last one. The reader sets `_head = tail`, skipping all earlier segments — bytes are permanently lost and their buffers leak. Fix: add a `state.Head` field written once by the first publication.

**2. `ComputeReadResult()` called on the writer thread.** In `MaybeSignalReaderAwaiter` (§8.2), the writer does `_reader._readAwaiter.SetResult(ComputeReadResult())`. Building the `ReadResult` reads and writes reader-local fields (`_head`, `_headConsumedOffset`, `_readInProgress`, `_lastReturnedBuffer`) from the writer thread, violating the single-writer-per-field invariant that the entire memory-ordering argument depends on. Fix: signal with a minimal sentinel value (just `isCanceled`/`isCompleted` flags); let the reader build the actual `ReadResult` on its own thread when the continuation resumes.

**3. Reader `Complete` leaks buffers with no specified cleanup path.** The spec acknowledges that segments published after reader completion are never retired, and claims they're "freed when `SpscPipe` is GC'd." But no finalizer or disposal logic is specified. The `BufferHolder` refcount never reaches zero, so `IMemoryOwner<byte>` instances are never returned to the pool — a real resource leak under repeated pipe lifecycles. Fix: specify explicit cleanup in `SpscPipe.Reset()` and/or a finalizer that force-returns outstanding holders and buffers.
