# Cross-thread `Complete` — Design

**Date:** 2026-05-10
**Status:** Spec (pre-implementation). Relaxes the SPSC threading contract on `PipeWriter.Complete` and `PipeReader.Complete` to allow invocation from any thread, matching `System.IO.Pipelines` (BCL) behavior.

## Top-level key takeaways

- `PipeWriter.Complete` and `PipeReader.Complete` may be invoked from any thread. The existing self-`Complete` (owner-thread) usage continues to work unchanged. All other members remain SPSC (single-producer / single-consumer).
- First-call wins. Subsequent `Complete` calls are silent no-ops and discard their `exception` argument — same as BCL `PipeCompletion.TryComplete`.
- Mechanism: one new ref-typed field per side (`_writerCompletion`, `_readerCompletion`) holds either an `ExceptionDispatchInfo` capture or a `s_completedSuccessfully` sentinel. `Complete` does `Interlocked.CompareExchange` against null — that CAS *is* the first-wins gate.
- Wake/observation rides on the existing `PipelyAwaiter._state` machine. A new `CompleteFlag` bit (`0b100`, alongside `Inactive`/`Pending` and `CancelFlag`) is set via `Interlocked.Or` on both awaiter state words. Slow-path methods (`FlushAsync`, `ReadAsync`, `TryRead`) extend their existing CAS drain to drain `CompleteFlag` — no separate `Volatile.Read`. Lost-wakeup re-checks (`ParkFlushAwaiter`, `ParkReadAwaiter`) extend symmetrically.
- Three new `Volatile.Read` sites — at `GetMemory`, `Advance`, `Splice` gates on the writer side. These three methods have no Interlocked op anywhere in the body, so the gate read needs an explicit acquire fence. Every other gate either already runs after an Interlocked op (gate-after-fence is a plain read) or folds into an existing CAS drain. `Volatile.Read` use here is a deliberate exception to the project's general avoidance — comments at each site spell out *why* and warn against propagating the pattern.
- `AdvanceTo` (reader) does not need `Volatile.Read`. Both branches gate after an Interlocked op already in the method: the trivial branch's gate sits after `_pipe.PublishReaderState()` (`Interlocked.Exchange` inside `TripleBuffer.Publish`); the non-trivial branch's gate sits after the `TryAcquire` block at line 136 (`Volatile.Read` / `Interlocked.Exchange` inside `TripleBuffer.TryAcquire`).
- The existing `if (_pipe._reader.ReaderCompleted) throw` / `WriterCompleted` plain-bool gates at the top of every hot-path method are retained as fast-paths catching post-deferred-handler calls. They cost a single branch and zero fences in the common (uncompleted) case.
- Terminal-state publication (the original `Complete` body's `LastPublished*State` and triple-buffer `Publish`) is no longer done synchronously inside `Complete`. It runs as a *deferred handler* on the owner thread's first post-Complete hot-path call, triggered by observing `CompleteFlag`. This lets `Complete` itself be uniform across all caller threads — no thread-identity tests, no race risk against in-flight owner-thread hot-path activity. Peers observe completion via `CompleteFlag` and `_completion`, not via the triple buffer's terminal state, so they never see a window where completion is requested but not published.

## Section 1 — Background and motivation

The current Pipely contract requires `Complete` to be invoked on the owning thread (writer thread for `PipeWriter.Complete`, reader thread for `PipeReader.Complete`). This is stricter than `System.IO.Pipelines.Pipe.CompleteWriter` / `CompleteReader`, which take an internal `SyncObj` lock and tolerate cross-thread invocation. The BCL contract's incidental robustness lets common idioms work — completing the writer from a timeout handler, a linked `CancellationToken` callback, or a downstream-error path — without coordinating back to the writer thread.

Pipely's current advice is to call `CancelPendingFlush` / `CancelPendingRead` from the third thread, observe the cancellation on the owner thread, and call `Complete` from there. That works but adds a thread hop and complicates straightforward error-propagation patterns. Migration friction is real: callers porting from `System.IO.Pipelines` hit `Complete`-from-third-thread and either misuse it (corrupting SPSC state) or rewrite their cancellation flow.

The goal of this change is to support cross-thread `Complete` with full BCL behavioral parity, while preserving Pipely's lock-free hot path.

The constraints, in order of priority:

1. No locks anywhere — including in the new `Complete` path.
2. `Volatile.Read` / `Volatile.Write` are tolerated only where strictly required for correctness, with explicit per-site justification. They are avoided as a general policy in this project to prevent drift toward LMAX/disruptor-class designs that have proven hard to maintain.
3. 100% behavioral parity with BCL: any peer call that strictly starts after `Complete` returns must observe completion; any owner-side hot-path call after `Complete` must throw immediately with the user's exception (or the documented `InvalidOperationException`).
4. The new code path should not regress steady-state throughput or latency on the existing self-`Complete` flow.

## Section 2 — Contract change

### 2.1 — Public-API surface diff

```csharp
// PipeWriter
/// <summary>
/// Marks writing as complete and (eventually) publishes the terminal writer state to the reader.
/// </summary>
/// <remarks>
/// Safe to call from any thread, in contrast to other members of <see cref="PipeWriter"/>.
/// First call wins; subsequent calls are silent no-ops, and their <paramref name="exception"/>
/// argument is discarded.
/// </remarks>
public override void Complete(Exception? exception = null);
```

```csharp
// PipeReader
/// <summary>
/// Marks reading as complete and (eventually) publishes the terminal reader state to the writer.
/// </summary>
/// <remarks>
/// Safe to call from any thread, in contrast to other members of <see cref="PipeReader"/>.
/// First call wins; subsequent calls are silent no-ops, and their <paramref name="exception"/>
/// argument is discarded.
/// </remarks>
public override void Complete(Exception? exception = null);
```

The type-level remarks on `PipeWriter` and `PipeReader` (currently "All members must be invoked on a single producer/consumer thread, including `Complete`") are amended: `Complete` is removed from the SPSC-restricted member list, joining `CancelPendingFlush` / `CancelPendingRead` as the multi-thread-safe surface.

### 2.2 — Behavioral guarantees

| Guarantee | BCL | Pipely (post-change) |
|---|---|---|
| Any thread may call `Complete` | ✓ | ✓ (new) |
| First call wins; second call's exception discarded | ✓ via `TryComplete` | ✓ via `Interlocked.CompareExchange` |
| Multiple concurrent `Complete` calls are serialized | ✓ via `lock` | ✓ via CAS first-wins |
| Owner-side hot-path call after `Complete` throws | ✓ (lock-protected) | ✓ (gate observes `CompleteFlag` via Interlocked-fenced read) |
| Peer call started after `Complete` returns observes completion | ✓ (lock release happens-before) | ✓ (`CompleteFlag` set before `Complete` returns; peer's hot-path gate observes via Interlocked drain) |
| Parked owner is woken on `Complete` | ✓ (callbacks invoked under lock) | ✓ (existing signaler logic, extended to release-on-`CompleteFlag`) |
| Original exception preserved with full stack via `ExceptionDispatchInfo` | ✓ | ✓ (same `ExceptionDispatchInfo.Capture` → `Throw` pattern) |

No behavioral guarantee is lost.

## Section 3 — Architecture

### 3.1 — New state

Two new ref-typed fields on `Pipe`:

```csharp
internal sealed partial class Pipe
{
    // ...existing state...

    // Completion sentinel for the writer side. Holds either an ExceptionDispatchInfo
    // (faulted completion) or s_completedSuccessfully (graceful completion). Null until
    // PipeWriter.Complete is first called. Installed via Interlocked.CompareExchange —
    // first-wins; subsequent installers see non-null and silently no-op.
    private object? _writerCompletion;

    // Symmetric, for the reader side.
    private object? _readerCompletion;

    // Sentinel for graceful completion (no exception). Reference-equality compared.
    private static readonly object s_completedSuccessfully = new();
}
```

These fields are written rarely (once per pipe lifetime, by whichever thread wins the `Complete` race) and read only on the throw path (when a hot-path gate has already observed `CompleteFlag`). They are not on the steady-state hot path. Cache-line placement is therefore not load-bearing, but they should be grouped near other rarely-written shared state rather than embedded in `WriterFields` / `ReaderFields` (which are owner-thread-local hot regions).

### 3.2 — `CompleteFlag` bit in `PipelyAwaiter._state`

The existing state-bit layout in `PipelyAwaiter`:

```
bit 0:    StateMask (0 = Inactive, 1 = Pending)
bit 1:    CancelFlag (sticky cancel request)
bit 2..n: ParkCount/version (unused for this change)
```

Add:

```
bit 2:    CompleteFlag (sticky completion request)
```

Bit assignment chosen to keep `CancelFlag` and `CompleteFlag` independently testable and CAS-drainable in the same word, mirroring the cancel-flag drain idiom already in `FlushAsync` / `ReadAsync` / `TryRead`.

Both `_readAwaiter._state` and `_flushAwaiter._state` get the bit. A `Complete` from either side sets the bit on **both** awaiter state words:

- Reader-side `Complete` sets `CompleteFlag` in `_readAwaiter._state` (drained by `ReadAsync` / `TryRead` / `AdvanceTo` post-fence reads) and in `_flushAwaiter._state` (drained by `FlushAsync` / `ParkFlushAwaiter`'s CAS).
- Writer-side `Complete` sets `CompleteFlag` in `_flushAwaiter._state` (drained by `FlushAsync` / `ParkFlushAwaiter`) and in `_readAwaiter._state` (drained by `ReadAsync` / `TryRead`).

The two `Interlocked.Or` calls are independent and need no ordering relative to each other — both bits are ultimately consulted, and each bit's setter ensures the corresponding CAS drain catches it.

### 3.3 — `Complete` body (uniform across all caller threads)

```csharp
public override void Complete(Exception? exception = null)
{
    var captured = exception != null
        ? (object)ExceptionDispatchInfo.Capture(exception)
        : s_completedSuccessfully;

    // First-wins gate. Matches BCL PipeCompletion.TryComplete idempotency:
    // if non-null is returned, someone (self or third party) already completed —
    // silently discard our exception and return.
    if (Interlocked.CompareExchange(ref _pipe._readerCompletion, captured, null) != null)
        return;

    // We won. Publish CompleteFlag to both awaiter state words. The two Or's are
    // independent; either order is correct.
    Interlocked.Or(ref _pipe._readAwaiter._state,  PipelyAwaiter<ReadResult>.CompleteFlag);
    Interlocked.Or(ref _pipe._flushAwaiter._state, PipelyAwaiter<FlushResult>.CompleteFlag);

    // Wake parked awaiters via the existing signal logic. CompleteFlag is a release-
    // from-Pending reason; the signaler transitions Pending -> Inactive and delivers
    // the result/exception to the parked task.
    _pipe.SignalReadAwaiterIfPending();
    _pipe.SignalFlushAwaiterIfPending();
}
```

The writer-side `Complete` is symmetric: CAS into `_writerCompletion`, set `CompleteFlag` on both state words, signal both awaiters.

Critical property: this body is safe to run from **any** thread because every memory mutation goes through `Interlocked` operations on Pipe-level shared state. It does **not** touch `_writer.*` or `_reader.*` fields (those are owner-thread-local), and it does **not** publish to `_writerTb` / `_readerTb` (those are SPSC and writing to them from a non-owner thread races with in-flight `FlushAsync` / `AdvanceTo`).

### 3.4 — Deferred handler

The original `Complete` body did three additional things that the new uniform body does **not**:

1. Set `_writer.WriterCompleted = true` / `_reader.ReaderCompleted = true` (plain bool, owner-thread-only write).
2. Build a terminal `WriterState` / `ReaderState` snapshot from owner-thread-local state and write it to `ProducerSlot()`.
3. Call `_writerTb.Publish()` / `_readerTb.Publish()`.

These three steps are owner-thread-only — running them from a third thread races with concurrent owner-thread hot-path activity. They are now performed by a **deferred handler** that runs on the owner thread's first post-`Complete` hot-path call, triggered by the gate observing `CompleteFlag`.

```csharp
// Reader-side deferred handler. Called by reader-side hot-path gates (post-fence)
// when CompleteFlag is observed and ReaderCompleted is still false. Idempotent:
// the second observer sees ReaderCompleted == true and skips.
private void RunReaderDeferredHandler()
{
    if (_reader.ReaderCompleted) return;     // already ran; subsequent observer
    _reader.ReaderCompleted = true;

    var snapshot = new ReaderState
    {
        HeadSegment         = null,
        TotalConsumed       = _reader.TotalConsumed,
        TotalExamined       = _reader.TotalExamined,
        IsCompleted         = true,
        CompletionException = (_readerCompletion as ExceptionDispatchInfo)?.SourceException,
    };
    _readerTb.ProducerSlot() = snapshot;
    _readerTb.Publish();
    _reader.LastPublishedReaderState = snapshot;
}
```

The handler is idempotent and reentrancy-safe via the `ReaderCompleted` self-check.

`Throw`-from-completion is a separate helper:

```csharp
[DoesNotReturn]
private static void ThrowFromCompletion(object completion)
{
    if (completion is ExceptionDispatchInfo edi) edi.Throw();
    throw new InvalidOperationException("Reading is completed.");
    //                                  ^ or "Writing is completed." for writer side
}
```

### 3.5 — Why peers don't need terminal state to be published to observe completion

Existing call sites that read `LastAcquiredReaderState.IsCompleted` / `.CompletionException` (e.g., `FlushAsync` line 76, `ParkFlushAwaiter` line 218) all currently return / throw based on those values. Under the new design, the writer's CAS drain on `_flushAwaiter._state` (extended to drain `CompleteFlag`) catches reader-completion **before** these triple-buffer-based checks are consulted. So even though the triple-buffer terminal state hasn't been published yet (deferred handler hasn't run), the writer correctly throws via `_readerCompletion` from the drain.

Symmetric on the reader side: `ReadAsync` / `TryRead`'s extended cancel-flag drain catches writer-completion via `CompleteFlag` in `_readAwaiter._state` before any consult of `LastAcquiredWriterState`.

The terminal-state publish (when the owner's deferred handler eventually runs) brings the cached / triple-buffer state in line with `_completion`. This matters for code paths that read `LastAcquired*State` *outside* the CAS drain (e.g., `BufferedBytes` accessor). After the deferred handler has run, those paths see consistent terminal state.

## Section 4 — Per-method changes

### 4.1 — Writer-side hot-path methods

#### `GetMemory` / `Advance` / `Splice` — Volatile.Read gate (the three sites)

All three currently begin with:

```csharp
if (_pipe._writer.WriterCompleted) throw new InvalidOperationException("Writing is completed.");
```

After the change:

```csharp
if (_pipe._writer.WriterCompleted) throw new InvalidOperationException("Writing is completed.");

// Cross-thread Complete gate. Volatile.Read is required here because this method
// performs no Interlocked operation anywhere in its body, so a plain read of
// _flushAwaiter._state would be vulnerable to JIT hoisting / ARM64 weak-memory
// staleness and could miss a CompleteFlag bit set by a third-thread Writer.Complete.
//
// This is one of three Volatile.Read sites added by the cross-thread-Complete change
// (the other two are in Advance and Splice). Volatile.Read is otherwise avoided in
// this project as a matter of policy — please do not propagate this pattern to other
// methods. FlushAsync, ReadAsync, TryRead, and AdvanceTo all observe CompleteFlag
// via existing Interlocked machinery and do not need a separate Volatile.Read.
//
// Cost: one acquire load per call. x86-64: emits a plain MOV (TSO + compiler
// barrier). ARM64: emits LDAR. Both are vastly cheaper than an Interlocked op and
// well below the noise floor for typical workloads.
if ((Volatile.Read(ref _pipe._flushAwaiter._state) & PipelyAwaiter<FlushResult>.CompleteFlag) != 0)
{
    _pipe.RunWriterDeferredHandler();
    Pipe.ThrowFromWriterCompletion(_pipe._writerCompletion!);
}
```

#### `FlushAsync` — extended CAS drain

The existing cancel-flag drain at lines 80-87:

```csharp
while (true)
{
    int oldV = _pipe._flushAwaiter._state;
    if ((oldV & PipelyAwaiter<FlushResult>.CancelFlag) == 0) break;
    int desired = oldV & ~PipelyAwaiter<FlushResult>.CancelFlag;
    if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
        return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: true));
}
```

becomes:

```csharp
while (true)
{
    int oldV = _pipe._flushAwaiter._state;

    // CompleteFlag check first — sticky and authoritative; consulting other flags
    // after this is unnecessary work. Note: read of oldV is post-fence relative to
    // the Interlocked.Exchange in TryAcquire (Pipe.Writer.cs:73), so a plain read
    // suffices here.
    if ((oldV & PipelyAwaiter<FlushResult>.CompleteFlag) != 0)
    {
        _pipe.RunWriterDeferredHandler();
        Pipe.ThrowFromWriterCompletion(_pipe._writerCompletion!);
    }

    if ((oldV & PipelyAwaiter<FlushResult>.CancelFlag) == 0) break;
    int desired = oldV & ~PipelyAwaiter<FlushResult>.CancelFlag;
    if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
        return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: true));
}
```

`CompleteFlag` is sticky — never cleared by drain. Subsequent iterations of the loop (if any) re-observe it and re-throw.

`ParkFlushAwaiter`'s lost-wakeup re-check (Pipe.Writer.cs:213-252) gets a symmetric `CompleteFlag` check after its `TryAcquire` (line 214), with a CAS that transitions Pending → Inactive when `CompleteFlag` is observed and delivers the completion exception via `_core.SetException`.

#### Plain-bool gates at line 29 / 60 / 70 / 288 — retained as fast-path

Each writer-side method's existing `if (_pipe._writer.WriterCompleted) throw` remains. It's a plain-bool check on an owner-thread-only field, so it sees only the deferred-handler-published value (set by the owner's first post-`Complete` hot-path call). It catches subsequent calls cheaply, after which the `Volatile.Read` (for `GetMemory`/`Advance`/`Splice`) or CAS drain (for `FlushAsync`) becomes redundant for that call but harmless.

### 4.2 — Reader-side hot-path methods

#### `ReadAsync` / `TryRead` — extended CAS drain

Same pattern as `FlushAsync`: the existing cancel-flag drain at Pipe.Reader.cs:42-50 (and 78-89 for `TryRead`) extends to drain `CompleteFlag` first. On observation, runs the reader deferred handler and throws via `_readerCompletion`.

`ParkReadAwaiter`'s lost-wakeup re-check extends symmetrically.

The non-trivial behavior wrinkle: after `Writer.Complete` (no exception), `ReadAsync` should return a `ReadResult` with `IsCompleted = true` and any remaining buffered data, **not** throw. The drain logic distinguishes:

```csharp
if ((oldV & PipelyAwaiter<ReadResult>.CompleteFlag) != 0)
{
    _pipe.RunReaderDeferredHandlerIfNeeded();   // sets ReaderCompleted-style fields if needed
    var completion = _pipe._writerCompletion!;
    if (completion is ExceptionDispatchInfo edi) edi.Throw();
    // s_completedSuccessfully — graceful close, return ReadResult with IsCompleted=true.
    _pipe._reader.ReadPending = true;
    return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: false));
    // BuildReadResult synthesizes IsCompleted=true via _writerCompletion.
}
```

`BuildReadResult` is updated to consult `_writerCompletion` for the `isCompleted` flag in addition to `LastAcquiredWriterState.IsCompleted`. (Symmetric: `BuildFlushResult` consults `_readerCompletion`.)

#### `AdvanceTo` — gate-after-Interlocked, two new sites

The existing gate at line 106 (`if (_pipe._reader.ReaderCompleted) throw`) is **retained** as a fast-path catching post-deferred-handler calls. It's plain-bool, single-thread-visible, no fence.

Two new gates handle the cross-thread case:

**Trivial branch** (`consumedSeg == null && examinedSeg == null` at lines 112-116):

```csharp
if (consumedSeg == null && examinedSeg == null)
{
    _pipe.PublishReaderState();

    // Cross-thread Complete gate. _pipe.PublishReaderState() invokes
    // _readerTb.Publish() which performs Interlocked.Exchange — full memory barrier.
    // A plain read of _readAwaiter._state after PublishReaderState returns sees
    // the latest globally-visible value, including any CompleteFlag bit set by a
    // third-thread Reader.Complete. DO NOT hoist this gate to the top of the
    // method — the fence is what makes the plain read correct.
    if ((_pipe._readAwaiter._state & PipelyAwaiter<ReadResult>.CompleteFlag) != 0)
    {
        _pipe.RunReaderDeferredHandlerIfNeeded();
        Pipe.ThrowFromReaderCompletion(_pipe._readerCompletion!);
    }
    return;
}
```

**Non-trivial branch** (after the `TryAcquire` block at lines 131-135):

```csharp
// Refresh writer state for upper-bound validation.
if (_pipe._writerTb.TryAcquire())
{
    _pipe._reader.LastAcquiredWriterState = _pipe._writerTb.ConsumerSlot();
    _pipe.IntegrateAcquiredWriterState();
}

// Cross-thread Complete gate. _writerTb.TryAcquire() performs Volatile.Read on its
// fast path and Interlocked.Exchange on its slow path — both are acquire-or-stronger
// fences. A plain read of _readAwaiter._state here sees the latest globally-visible
// value, including any CompleteFlag bit set by a third-thread Reader.Complete. The
// gate must be after TryAcquire, not at the top of the method — the fence is what
// makes the plain read correct. DO NOT hoist.
if ((_pipe._readAwaiter._state & PipelyAwaiter<ReadResult>.CompleteFlag) != 0)
{
    _pipe.RunReaderDeferredHandlerIfNeeded();
    Pipe.ThrowFromReaderCompletion(_pipe._readerCompletion!);
}

if (consumedAbs < _pipe._reader.TotalConsumed
    || examinedAbs < _pipe._reader.TotalExamined
    || consumedAbs > examinedAbs
    || examinedAbs > _pipe._reader.LastAcquiredWriterState.TotalWritten)
{
    throw new InvalidOperationException("AdvanceTo position out of range");
}

// ... existing state mutation and PublishReaderState ...
```

## Section 5 — Interactions with existing invariants

### 5.1 — `SignalFlushIfBackpressureRelieved` may now run post-completion (trivial branch only)

The comment at Pipe.cs:387-388 currently asserts:

> Note: no `|| _reader.ReaderCompleted` clause — AdvanceTo's entry guard throws if `_reader.ReaderCompleted`, so this code path never runs post-completion. Reader.Complete uses SignalFlushAwaiterIfPending (unconditional).

After the change, the trivial branch's gate fires **after** `PublishReaderState()` (which calls `SignalFlushIfBackpressureRelieved`), so this code path **does** run post-completion in the third-thread `reader.Complete` scenario. The non-trivial branch's gate fires before `PublishReaderState`, preserving the invariant in that path.

Verification that running `SignalFlushIfBackpressureRelieved` post-completion is benign:

- The fast-path return at Pipe.cs:377 (`if (_state & StateMask) != Pending`) makes redundant wake attempts no-ops when the writer was already woken by the third-thread `Complete`'s signal.
- Even in the path where `SignalFlushIfBackpressureRelieved` does CAS the awaiter state, it transitions Pending → Inactive and calls `DeliverFlushResult`, which reads `_reader.LastPublishedReaderState`. If the reader's deferred handler has run, this state is terminal — the writer's parked task gets the right `IsCompleted` result. If the deferred handler hasn't run yet, `DeliverFlushResult` delivers a non-terminal state — but the writer's continuation re-checks via the lost-wakeup path in `ParkFlushAwaiter` (extended to drain `CompleteFlag`) and throws via `_readerCompletion` regardless.
- Net effect: in the gap window, a parked writer might be woken with a non-terminal `FlushResult` then immediately throw via the lost-wakeup re-check. Slight inefficiency, no correctness issue.

The comment at Pipe.cs:387-388 is updated to reflect the new invariants:

```
// Note: this code path may run post-completion in the third-thread Reader.Complete
// scenario, when AdvanceTo's trivial branch (both null) fires its gate after
// PublishReaderState. State-mask guards make redundant wake attempts no-ops, and
// the writer's CAS drain catches CompleteFlag independently. See spec
// 2026-05-10-cross-thread-complete-design.md §5.1.
```

### 5.2 — `BuildReadResult` / `BuildFlushResult` consult `_completion`

Both helpers are updated to synthesize `IsCompleted` from `_writerCompletion` / `_readerCompletion` in addition to `LastAcquired*State.IsCompleted`. This ensures peers see `IsCompleted = true` immediately after a third-thread `Complete`, even before the deferred handler has published terminal state.

### 5.3 — `Pipe.Reset` / `Pipe.Dispose` clear completion state

`_writerCompletion` and `_readerCompletion` are reset to `null` in `Pipe.Reset` so a recycled pipe starts fresh. `Pipe.Dispose` does the same.

### 5.4 — `BufferedBytes` and other read-only accessors

`PipeWriter.BufferedBytes` (Pipe.Writer.cs:191-192) reads `LastAcquiredReaderState.TotalConsumed`. This continues to work — `TotalConsumed` is unaffected by completion.

`PipeWriter.UnflushedBytes` reads `LastPublishedWriterState.TotalWritten`. After self-`Complete`, this is the terminal `TotalWritten`. After third-thread `Complete` (deferred handler not yet run), this is the pre-completion `TotalWritten`. Both values are technically correct: "bytes written but not yet flushed" is well-defined regardless of completion state.

## Section 6 — Edge cases

### 6.1 — Concurrent `Complete` calls

Two threads (or the owner and a third thread) call `Complete` concurrently with different exceptions:

- Both compute their `captured` values and race on `Interlocked.CompareExchange(ref _completion, captured, null)`.
- One CAS returns null (winner installed its captured value); the other returns the winner's value (loser sees non-null) and silently returns.
- The winner sets `CompleteFlag` bits and signals.
- The loser's exception is discarded — matches BCL `TryComplete`.

Note: there is no requirement that the winner's `CompleteFlag` bits and signals are observed by the loser; the loser exits before doing any further work.

### 6.2 — `Complete` called while owner is mid-`FlushAsync` / `ReadAsync` etc.

The owner-thread method is in some intermediate state (e.g., having just published a `WriterState` snapshot and about to park). Third thread calls `Complete`.

- Third thread's `Interlocked.Or` sets `CompleteFlag`.
- Third thread's `SignalFlushAwaiterIfPending` either wakes a parked owner (if owner is already in Pending state) or no-ops (if owner is Inactive or transitioning).
- Owner's continued execution: the next Interlocked op (CAS drain in `FlushAsync`, or the `Park*` lost-wakeup re-check) observes `CompleteFlag`. Throws.

The signal-wakeup race is handled by the existing `PipelyAwaiter` state machine: the signal fails to find Pending (owner hasn't transitioned yet), but the owner's lost-wakeup re-check fires the throw on its own subsequent iteration. This is the same race shape that already exists for `CancelPending*`; the design is reused.

### 6.3 — `Complete` called multiple times on the owner thread

Standard idempotency. First call wins; second and subsequent return silently. Behavior identical to BCL.

### 6.4 — `Complete` called from a continuation of a `ReadAsync` / `FlushAsync`

The continuation runs on whichever thread the awaiter scheduled it on. If that's not the owner thread, it's a third-thread `Complete` from Pipely's perspective — the new contract handles it. If it's the owner thread (e.g., `RunContinuationsAsynchronously = false`, sync-completed task), it's a self-`Complete`. Both paths are valid and produce the same observable behavior.

### 6.5 — Reader observing writer-completion while writer is mid-flush

Third thread calls `writer.Complete()` while the writer thread is mid-`FlushAsync` (between `_writerTb.ProducerSlot()` write and `_writerTb.Publish()`). The `CompleteFlag` is set on `_readAwaiter._state`. The reader's next `ReadAsync` / `TryRead` drain catches it and either throws (faulted) or returns `IsCompleted=true` with whatever data was previously visible (graceful).

The data that the writer was about to publish via the in-flight `FlushAsync` may or may not become visible to the reader — depends on the race. In either case, the reader sees a consistent prefix (matches BCL: a `Complete`-vs-`Flush` race is not specified to expose the in-flight bytes).

## Section 7 — Testing strategy

Three groups, mirroring the existing test layout under `tests/`.

### 7.1 — `PipeCompleteCrossThreadTests` (new file)

Basic contract:

- `Complete_FromArbitraryThread_DoesNotThrow` — assert that `Task.Run(() => writer.Complete()).Wait()` and the reader-side equivalent both succeed.
- `Complete_TwiceFromSameThread_SecondIsNoOp` — first call sets state; second call's exception is discarded; only first exception is rethrown by subsequent reader-side calls.
- `Complete_TwiceFromDifferentThreads_SecondIsNoOp` — concurrent `Complete` from two threads with different exceptions; verify only one exception is rethrown by subsequent calls (use `Interlocked.Increment` / `Volatile.Read` test scaffolding to identify which thread won).
- `Complete_GracefulVsFaulted_CorrectFlag` — `Complete()` followed by reader's `ReadAsync` returns `IsCompleted=true`, no exception. `Complete(ex)` followed by `ReadAsync` throws `ex`.

Owner-side throw immediacy:

- `Writer_GetMemoryAfterThirdThreadComplete_Throws` — third-thread `writer.Complete()`; await a `Thread.MemoryBarrier`-equivalent synchronization point; writer thread calls `GetMemory(1)`; assert `InvalidOperationException`.
- Same for `Advance`, `Splice`, `FlushAsync` (writer side); `ReadAsync`, `TryRead`, `AdvanceTo` (reader side).
- `Writer_FlushAsyncAfterThirdThreadComplete_RethrowsCapturedException` — third-thread `reader.Complete(ex)`; writer's `FlushAsync` throws `ex` via the captured `ExceptionDispatchInfo` (full original stack frames preserved — assert by inspecting `ex.StackTrace`).

Peer observation:

- `Reader_ReadAsyncAfterWriterCompleted_ReturnsIsCompletedTrue` — third-thread `writer.Complete()`; reader's `ReadAsync` returns synchronously with `result.IsCompleted == true`.
- `Reader_ReadAsyncAfterWriterCompletedWithException_Throws` — third-thread `writer.Complete(ex)`; reader's `ReadAsync` throws `ex`.

Parked-owner wake:

- `Reader_ParkedReadAsync_WokenByThirdThreadWriterComplete` — reader awaits `ReadAsync`; third thread calls `writer.Complete()`; the awaited task completes with `IsCompleted=true`.
- `Writer_ParkedFlushAsync_WokenByThirdThreadReaderComplete` — writer awaits `FlushAsync` (under backpressure); third thread calls `reader.Complete()`; the awaited task throws / returns appropriately.

### 7.2 — `PipeCompleteCrossThreadStressTests` (new file under `tests/Pipe.Stress/`)

Two scenarios, each with a 5-10s wall-clock budget:

- **Concurrent `Complete` vs hot-path activity.** The writer thread runs a `GetMemory`/`Advance`/`FlushAsync` loop; the reader thread runs a `ReadAsync`/`AdvanceTo` loop. A third thread, after a randomized delay, calls `writer.Complete(new InvalidOperationException("test"))` exactly once. Assert: writer's loop terminates with the test exception; reader's loop terminates with the test exception (rethrown via `ExceptionDispatchInfo`); no other exceptions. Repeat with reader-side third-thread `Complete`.
- **Race against `CancelPendingFlush` / `CancelPendingRead`.** Same setup, but the third thread also fires `CancelPendingRead` / `CancelPendingFlush` interleaved with `Complete`. Assert: no deadlocks, no spurious cancellations, terminal exception is the `Complete`-supplied one.

### 7.3 — Regression / invariant verification

- `SignalFlushIfBackpressureRelieved_PostCompletion_IsBenign` — directly invoke the trivial-branch path post-third-thread-`reader.Complete` with a parked writer; assert the writer wakes with the right exception (no hang, no wrong result).
- `Reset_ClearsCompletionFields` — call `Complete`, then `Pipe.Reset`; assert `_writerCompletion` / `_readerCompletion` are null again and a fresh `Complete` cycle works.
- `BufferedBytes_AfterThirdThreadComplete_ReturnsConsistentValue` — pin the documented "TotalConsumed-based; insensitive to completion" property.

### 7.4 — Pre-existing test compatibility

All existing `Complete`-related tests (self-`Complete` paths) must continue to pass without modification. A regression run is part of the implementation plan's verification step.

## Section 8 — Out of scope

- Surfacing the completion exception via a separate observable (e.g., a `Task` representing the completion). Callers can already observe via the next `ReadAsync` / `FlushAsync`. Adding a separate observable would expand public API without addressing a documented use case.
- Cross-thread invocation of any other method (e.g., `GetMemory` from an arbitrary thread). The SPSC contract for everything except `Complete` and `CancelPending*` is preserved.
- Removing the self-`Complete` fast paths (the line 106 / 29 / 60 / 70 / 288 plain-bool gates). They cost a single branch and provide a clear hot-path optimization for the post-deferred-handler case.
- Replacing the awaiter-state state machine with a different primitive. The bit-packed `_state` int is load-bearing for cancel-flag drains and now for complete-flag drains; expanding it isn't motivated by this change.
