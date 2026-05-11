# Cross-thread `Complete` — Design

**Date:** 2026-05-10 (revised 2026-05-11, revised 2026-05-11)
**Status:** Spec (pre-implementation). Relaxes the SPSC threading contract on `PipeWriter.Complete` and `PipeReader.Complete` to allow invocation from any thread, matching `System.IO.Pipelines` (BCL) behavior.

**2026-05-11 revision (round 2)**: replaces the single `CompleteFlag` bit with two distinct flags `WriterCompleteFlag` / `ReaderCompleteFlag` so drains can distinguish self- vs peer-completion (caught by code review — a single bit on both awaiter words couldn't tell which side completed, leading to wrong dispatch and a null-deref on the unset `_*Completion` field). Also factors out a `DeliverReadResult` helper to match `DeliverFlushResult`, and corrects the `PipelyAwaiter._state` bit-layout description.

## Top-level key takeaways

- `PipeWriter.Complete` and `PipeReader.Complete` may be invoked from any thread. The existing self-`Complete` (owner-thread) usage continues to work unchanged. All other members remain SPSC (single-producer / single-consumer).
- First-call wins. Subsequent `Complete` calls are silent no-ops and discard their `exception` argument — same as BCL `PipeCompletion.TryComplete`.
- Mechanism: one new ref-typed field per side (`_writerCompletion`, `_readerCompletion`) holds either an `ExceptionDispatchInfo` capture or a `s_completedSuccessfully` sentinel. `Complete` does `Interlocked.CompareExchange` against null — that CAS *is* the first-wins gate.
- Wake/observation rides on the existing `PipelyAwaiter._state` machine. Two new flag bits — `WriterCompleteFlag` (`0b100`) and `ReaderCompleteFlag` (`0b1000`), alongside the existing `Inactive`/`Pending` and `CancelFlag` — are set via `Interlocked.Or` on both awaiter state words. The two-flag design lets each drain distinguish self-completion ("my side") from peer-completion ("the other side"), dispatching to throw vs synthesized success accordingly. Slow-path methods (`FlushAsync`, `ReadAsync`, `TryRead`) extend their existing CAS drain to handle both flags — no separate `Volatile.Read`. Lost-wakeup re-checks (`ParkFlushAwaiter`, `ParkReadAwaiter`) extend symmetrically.
- Three new `Volatile.Read` sites — at `GetMemory`, `Advance`, `Splice` gates on the writer side. These three methods have no Interlocked op anywhere in the body, so the gate read needs an explicit acquire fence. Every other gate either already runs after an Interlocked op (gate-after-fence is a plain read) or folds into an existing CAS drain. `Volatile.Read` use here is a deliberate exception to the project's general avoidance — comments at each site spell out *why* and warn against propagating the pattern.
- `AdvanceTo` (reader) does not need `Volatile.Read`. Both branches gate after an Interlocked op already in the method: the trivial branch's gate sits after `_pipe.PublishReaderState()` (`Interlocked.Exchange` inside `TripleBuffer.Publish`); the non-trivial branch's gate sits after the `TryAcquire` block (`Volatile.Read` / `Interlocked.Exchange` inside `TripleBuffer.TryAcquire`).
- The existing `if (WriterCompleted) throw` / `if (ReaderCompleted) throw` plain-bool gates at the top of every hot-path method (Pipe.Writer.cs:29, 60, 70, 288; Pipe.Reader.cs:28, 66, 106) are **removed**. They previously caught post-deferred-handler calls but cost one branch per call on the steady-state hot path; the post-fence Interlocked-or-Volatile-Read gates already cover those cases at no measurable hot-path cost. The `WriterCompleted` / `ReaderCompleted` plain-bool fields remain (the deferred handler still sets them, and `Pipe.Reset` reads them — see §5.3).
- Terminal-state publication (the original `Complete` body's `LastPublished*State` and triple-buffer `Publish`) is no longer done synchronously inside `Complete`. It runs as a *deferred handler* on the owner thread's first post-Complete hot-path call, triggered by observing the relevant completion flag bit. This lets `Complete` itself be uniform across all caller threads — no thread-identity tests, no race risk against in-flight owner-thread hot-path activity. Peers observe completion via the completion flag bits and `_completion`, not via the triple buffer's terminal state, so they never see a window where completion is requested but not published.
- `Pipe.PublishReaderState` and `Pipe.SignalFlushIfBackpressureRelieved` are **decoupled**. Today the former tail-calls the latter; the new design has each `AdvanceTo` branch call them independently. The trivial branch (both `SequencePosition`s null) calls only `PublishReaderState` — no signaling, since nothing was consumed and backpressure cannot have relieved. This removes the only path by which `SignalFlushIfBackpressureRelieved` would run post-completion-request via `AdvanceTo`. (Symmetric refactor on the writer side if needed; investigation deferred to implementation — see §5.1.)
- `Pipe.DeliverFlushResult` and `Pipe.DeliverReadResult` (the signaler-side result-delivery helpers) consult `_readerCompletion` / `_writerCompletion` (post-fence plain read; the signaler paths have just executed an Interlocked op) and synthesize `IsCompleted=true` / rethrow the captured exception when set. This closes the residual race where a parked owner could be woken with a stale `IsCompleted=false` after a third-thread `Complete`.

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
| Owner-side hot-path call after `Complete` throws | ✓ (lock-protected) | ✓ (gate observes own-side completion flag via Interlocked-fenced read) |
| Peer call started after `Complete` returns observes completion | ✓ (lock release happens-before) | ✓ (peer-side completion flag set before `Complete` returns; peer's hot-path gate observes via Interlocked drain) |
| Parked owner is woken on `Complete` | ✓ (callbacks invoked under lock) | ✓ (existing signaler logic, extended to release on either completion flag) |
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

These fields are written rarely (once per pipe lifetime, by whichever thread wins the `Complete` race) and read only on the throw / dispatch path (when a hot-path gate has already observed a completion flag bit). They are not on the steady-state hot path. Cache-line placement is therefore not load-bearing, but they should be grouped near other rarely-written shared state rather than embedded in `WriterFields` / `ReaderFields` (which are owner-thread-local hot regions).

### 3.2 — Completion flag bits in `PipelyAwaiter._state`

The existing state-bit layout in `PipelyAwaiter` (verified against `PipelyAwaiter.cs:68-71`):

```
bit 0: StateMask (0 = Inactive, 1 = Pending)
bit 1: CancelFlag (sticky cancel request)
```

Bits 2+ are unused. `_parkCount` and the other diagnostic counters are separate `long` fields, not packed into `_state`.

Add two new bits:

```
bit 2: WriterCompleteFlag (sticky; set when Writer.Complete is called)
bit 3: ReaderCompleteFlag (sticky; set when Reader.Complete is called)
```

Two separate bits — not one — because each drain needs to distinguish *which side* completed in order to dispatch correctly. On the reader's `_readAwaiter._state`, observing `ReaderCompleteFlag` means "my own side has been completed; throw" (`_readerCompletion` is authoritative), while observing `WriterCompleteFlag` means "the peer completed; return `IsCompleted=true` or rethrow the peer's exception" (`_writerCompletion` is authoritative). A single bit cannot encode this, and the `_*Completion` field for the unset side is null on the relevant code path.

Both `_readAwaiter._state` and `_flushAwaiter._state` carry both flag bits. A `Complete` from either side sets *its* flag (only) on **both** awaiter state words:

- `Writer.Complete` sets `WriterCompleteFlag` in `_flushAwaiter._state` (writer-side self-throw via `FlushAsync` / `ParkFlushAwaiter`'s CAS; writer's `GetMemory`/`Advance`/`Splice` Volatile.Read gates) **and** in `_readAwaiter._state` (reader-side peer-completion via `ReadAsync` / `TryRead`'s CAS; wakes parked reader with `IsCompleted=true`).
- `Reader.Complete` sets `ReaderCompleteFlag` in `_readAwaiter._state` (reader-side self-throw via `ReadAsync` / `TryRead`'s CAS, `AdvanceTo`'s post-Interlocked gate) **and** in `_flushAwaiter._state` (writer-side peer-completion via `FlushAsync` / `ParkFlushAwaiter`'s CAS; wakes parked writer with `IsCompleted=true`).

The two `Interlocked.Or` calls per `Complete` are independent and need no ordering relative to each other.

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

    // We won. Publish ReaderCompleteFlag to both awaiter state words. The two Or's
    // are independent; either order is correct. (Writer.Complete would set
    // WriterCompleteFlag instead — see §3.2 for the per-flag dispatch semantics.)
    Interlocked.Or(ref _pipe._readAwaiter._state,  PipelyAwaiter<ReadResult>.ReaderCompleteFlag);
    Interlocked.Or(ref _pipe._flushAwaiter._state, PipelyAwaiter<FlushResult>.ReaderCompleteFlag);

    // Wake parked awaiters via the existing signal logic. The flag bit is a release-
    // from-Pending reason; the signaler transitions Pending -> Inactive and delivers
    // the result/exception to the parked task (dispatched via _readerCompletion or
    // _writerCompletion depending on which side called Complete — see §5.1.2).
    _pipe.SignalReadAwaiterIfPending();
    _pipe.SignalFlushAwaiterIfPending();
}
```

The writer-side `Complete` is symmetric: CAS into `_writerCompletion`, set `WriterCompleteFlag` on both state words, signal both awaiters.

Critical property: this body is safe to run from **any** thread because every memory mutation goes through `Interlocked` operations on Pipe-level shared state. It does **not** touch `_writer.*` or `_reader.*` fields (those are owner-thread-local), and it does **not** publish to `_writerTb` / `_readerTb` (those are SPSC and writing to them from a non-owner thread races with in-flight `FlushAsync` / `AdvanceTo`).

### 3.4 — Deferred handler

The original `Complete` body did three additional things that the new uniform body does **not**:

1. Set `_writer.WriterCompleted = true` / `_reader.ReaderCompleted = true` (plain bool, owner-thread-only write).
2. Build a terminal `WriterState` / `ReaderState` snapshot from owner-thread-local state and write it to `ProducerSlot()`.
3. Call `_writerTb.Publish()` / `_readerTb.Publish()`.

These three steps are owner-thread-only — running them from a third thread races with concurrent owner-thread hot-path activity. They are now performed by a **deferred handler** that runs on the owner thread's first post-`Complete` hot-path call, triggered by the gate observing the own-side completion flag.

```csharp
// Reader-side deferred handler. Called by reader-side hot-path gates (post-fence)
// when ReaderCompleteFlag is observed. Idempotent: the second observer sees
// ReaderCompleted == true and skips.
internal void RunReaderDeferredHandlerIfNeeded()
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

Throw-from-completion helpers (one per side; the message differs):

```csharp
[DoesNotReturn]
internal static void ThrowFromReaderCompletion(object completion)
{
    if (completion is ExceptionDispatchInfo edi) edi.Throw();
    throw new InvalidOperationException("Reading is completed.");
}

[DoesNotReturn]
internal static void ThrowFromWriterCompletion(object completion)
{
    if (completion is ExceptionDispatchInfo edi) edi.Throw();
    throw new InvalidOperationException("Writing is completed.");
}
```

### 3.5 — Why peers don't need terminal state to be published to observe completion

Existing call sites that read `LastAcquiredReaderState.IsCompleted` / `.CompletionException` (e.g., `FlushAsync` line 76, `ParkFlushAwaiter` line 218) all currently return / throw based on those values. Under the new design, the writer's CAS drain on `_flushAwaiter._state` (extended to dispatch on `ReaderCompleteFlag` for peer-completion) catches reader-completion **before** these triple-buffer-based checks are consulted. So even though the triple-buffer terminal state hasn't been published yet (deferred handler hasn't run), the writer correctly dispatches via `_readerCompletion` from the drain.

Symmetric on the reader side: `ReadAsync` / `TryRead`'s extended cancel-flag drain catches writer-completion via `WriterCompleteFlag` in `_readAwaiter._state` before any consult of `LastAcquiredWriterState`.

The terminal-state publish (when the owner's deferred handler eventually runs) brings the cached / triple-buffer state in line with `_completion`. This matters for code paths that read `LastAcquired*State` *outside* the CAS drain (e.g., `BufferedBytes` accessor). After the deferred handler has run, those paths see consistent terminal state.

## Section 4 — Per-method changes

### 4.1 — Writer-side hot-path methods

#### `GetMemory` / `Advance` / `Splice` — Volatile.Read gate (the three sites)

The existing plain-bool gate (`if (_pipe._writer.WriterCompleted) throw …`) at the top of each method is **removed**. The body now begins with a single gate that checks `WriterCompleteFlag` only — these methods do not throw on peer (reader) completion, mirroring today's behavior (`WriterCompleted` was the only gate today; reader-completion isn't checked here):

```csharp
// Cross-thread Complete gate. Volatile.Read is required here because this method
// performs no Interlocked operation anywhere in its body, so a plain read of
// _flushAwaiter._state would be vulnerable to JIT hoisting / ARM64 weak-memory
// staleness and could miss a WriterCompleteFlag bit set by a third-thread
// Writer.Complete.
//
// We test only WriterCompleteFlag here; ReaderCompleteFlag (peer completion) is
// not a reason for GetMemory/Advance/Splice to throw — peer completion surfaces
// to the writer via FlushAsync (per existing BCL/Pipely contract).
//
// This is one of three Volatile.Read sites added by the cross-thread-Complete change
// (the other two are in Advance and Splice). Volatile.Read is otherwise avoided in
// this project as a matter of policy — please do not propagate this pattern to other
// methods. FlushAsync, ReadAsync, TryRead, and AdvanceTo all observe completion
// flags via existing Interlocked machinery and do not need a separate Volatile.Read.
//
// Cost: one acquire load per call. x86-64: emits a plain MOV (TSO + compiler
// barrier). ARM64: emits LDAR. Both are vastly cheaper than an Interlocked op and
// well below the noise floor for typical workloads.
if ((Volatile.Read(ref _pipe._flushAwaiter._state) & PipelyAwaiter<FlushResult>.WriterCompleteFlag) != 0)
{
    _pipe.RunWriterDeferredHandlerIfNeeded();
    Pipe.ThrowFromWriterCompletion(_pipe._writerCompletion!);
}
```

The deferred handler (`RunWriterDeferredHandlerIfNeeded`) is idempotent (early-returns if `_writer.WriterCompleted` is already true), so subsequent post-Complete calls don't re-publish — they just re-read the bit and throw via `ThrowFromWriterCompletion`. The branch cost is a single test-and-branch on `_flushAwaiter._state`'s `WriterCompleteFlag` bit, predicted not-taken on the steady-state path.

#### `FlushAsync` — plain-bool gate removed, CAS drain extended (handles both flags)

The existing plain-bool gate at line 70 (`if (_pipe._writer.WriterCompleted) throw …`) is **removed**. The CAS drain at lines 80-87 is extended to dispatch on both completion flags:

```csharp
while (true)
{
    int oldV = _pipe._flushAwaiter._state;

    // Self-completion (Writer.Complete fired): throw. Sticky bit, never cleared.
    // Read of oldV is post-fence relative to the Interlocked.Exchange in
    // TryAcquire (Pipe.Writer.cs:73), so a plain read suffices here.
    if ((oldV & PipelyAwaiter<FlushResult>.WriterCompleteFlag) != 0)
    {
        _pipe.RunWriterDeferredHandlerIfNeeded();
        Pipe.ThrowFromWriterCompletion(_pipe._writerCompletion!);
    }

    // Peer-completion (Reader.Complete fired): return FlushResult(IsCompleted=true)
    // for graceful close, or rethrow the captured exception for faulted close.
    // This matches today's behavior at Pipe.Writer.cs:76 but observes via the flag
    // instead of relying on LastAcquiredReaderState being terminal.
    if ((oldV & PipelyAwaiter<FlushResult>.ReaderCompleteFlag) != 0)
    {
        var completion = _pipe._readerCompletion;     // post-fence plain read
        if (completion is ExceptionDispatchInfo edi) edi.Throw();
        // s_completedSuccessfully — graceful close.
        return new ValueTask<FlushResult>(new FlushResult(isCanceled: false, isCompleted: true));
    }

    if ((oldV & PipelyAwaiter<FlushResult>.CancelFlag) == 0) break;
    int desired = oldV & ~PipelyAwaiter<FlushResult>.CancelFlag;
    if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
        return new ValueTask<FlushResult>(_pipe.BuildFlushResult(isCanceled: true));
}
```

Both flags are sticky — never cleared by drain. Subsequent iterations of the loop (if any) re-observe them.

`ParkFlushAwaiter`'s lost-wakeup re-check (Pipe.Writer.cs:213-252) gets a symmetric two-flag check after its `TryAcquire` (line 214):

```csharp
int v = _pipe._flushAwaiter._state;
if ((v & PipelyAwaiter<FlushResult>.WriterCompleteFlag) != 0)
{
    // Self-completion observed lost-wakeup-style. CAS Pending→Inactive and deliver throw.
    while (true)
    {
        int oldV = _pipe._flushAwaiter._state;
        if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) break;
        int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
        if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
        {
            Interlocked.Increment(ref _pipe._flushAwaiter._lostWakeupResolvedCount);
            _pipe.RunWriterDeferredHandlerIfNeeded();
            _pipe._flushAwaiter._core.SetException(
                (_pipe._writerCompletion as ExceptionDispatchInfo)?.SourceException
                ?? new InvalidOperationException("Writing is completed."));
            return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
        }
    }
}
if ((v & PipelyAwaiter<FlushResult>.ReaderCompleteFlag) != 0)
{
    // Peer-completion observed lost-wakeup-style. CAS Pending→Inactive and deliver
    // either SetException (faulted reader) or SetResult(IsCompleted=true).
    while (true)
    {
        int oldV = _pipe._flushAwaiter._state;
        if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) break;
        int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
        if (Interlocked.CompareExchange(ref _pipe._flushAwaiter._state, desired, oldV) == oldV)
        {
            Interlocked.Increment(ref _pipe._flushAwaiter._lostWakeupResolvedCount);
            var completion = _pipe._readerCompletion;
            if (completion is ExceptionDispatchInfo edi)
                _pipe._flushAwaiter._core.SetException(edi.SourceException);
            else
                _pipe._flushAwaiter._core.SetResult(new FlushResult(isCanceled: false, isCompleted: true));
            return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
        }
    }
}
```

The existing lost-wakeup blocks for backpressure-release and lost-cancel follow these, unchanged in shape.

### 4.2 — Reader-side hot-path methods

#### `ReadAsync` / `TryRead` — plain-bool gate removed, CAS drain extended (handles both flags)

The plain-bool gates at Pipe.Reader.cs:28 / 66 (`if (_pipe._reader.ReaderCompleted) throw …`) are **removed**, following the same logic as the writer-side removals: the CAS drain catches all completion shapes, including post-deferred-handler calls. Each call pays one extra `Interlocked.CompareExchange` (the drain) on the rare second-throw path — negligible cost.

The existing cancel-flag drain at Pipe.Reader.cs:42-50 (and 78-89 for `TryRead`) extends to dispatch on both completion flags:

```csharp
while (true)
{
    int oldV = _pipe._readAwaiter._state;

    // Self-completion (Reader.Complete fired): throw via _readerCompletion.
    if ((oldV & PipelyAwaiter<ReadResult>.ReaderCompleteFlag) != 0)
    {
        _pipe.RunReaderDeferredHandlerIfNeeded();
        Pipe.ThrowFromReaderCompletion(_pipe._readerCompletion!);
    }

    // Peer-completion (Writer.Complete fired): return ReadResult(IsCompleted=true)
    // for graceful close (with any remaining buffered data), or rethrow the
    // captured exception for faulted close.
    if ((oldV & PipelyAwaiter<ReadResult>.WriterCompleteFlag) != 0)
    {
        var completion = _pipe._writerCompletion;       // post-fence plain read
        if (completion is ExceptionDispatchInfo edi) edi.Throw();
        // s_completedSuccessfully — graceful close, return ReadResult with IsCompleted=true.
        _pipe._reader.ReadPending = true;
        return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: false));
        // BuildReadResult synthesizes IsCompleted=true via _writerCompletion (see §5.2).
    }

    if ((oldV & PipelyAwaiter<ReadResult>.CancelFlag) == 0) break;
    int desired = oldV & ~PipelyAwaiter<ReadResult>.CancelFlag;
    if (Interlocked.CompareExchange(ref _pipe._readAwaiter._state, desired, oldV) == oldV)
    {
        _pipe._reader.ReadPending = true;
        return new ValueTask<ReadResult>(_pipe.BuildReadResult(isCanceled: true));
    }
}
```

`ParkReadAwaiter`'s lost-wakeup re-check extends symmetrically — same shape as the writer-side `ParkFlushAwaiter` extension in §4.1: two CAS-back-to-Inactive blocks, one per flag, dispatching to `SetException` (self-completion or peer-faulted) or `SetResult(IsCompleted=true)` (peer-graceful).

#### `AdvanceTo` — plain-bool gate removed; two new gate-after-Interlocked sites; `PublishReaderState`/`SignalFlushIfBackpressureRelieved` decoupled

The existing plain-bool gate at line 106 is **removed**. The two branches handle cross-thread completion via gates placed after Interlocked ops already present in the method.

`AdvanceTo` only checks `ReaderCompleteFlag` (own-side completion). Peer (writer) completion is not a reason for `AdvanceTo` to throw — symmetric to today's behavior where `AdvanceTo`'s only gate is `ReaderCompleted`.

**Trivial branch** (`consumedSeg == null && examinedSeg == null` at lines 112-116):

```csharp
if (consumedSeg == null && examinedSeg == null)
{
    _pipe.PublishReaderState();

    // Cross-thread Complete gate. _pipe.PublishReaderState() invokes
    // _readerTb.Publish() which performs Interlocked.Exchange — full memory barrier.
    // A plain read of _readAwaiter._state after PublishReaderState returns sees the
    // latest globally-visible value, including any ReaderCompleteFlag bit set by a
    // third-thread Reader.Complete. DO NOT hoist this gate to the top of the
    // method — the fence is what makes the plain read correct.
    if ((_pipe._readAwaiter._state & PipelyAwaiter<ReadResult>.ReaderCompleteFlag) != 0)
    {
        _pipe.RunReaderDeferredHandlerIfNeeded();
        Pipe.ThrowFromReaderCompletion(_pipe._readerCompletion!);
    }

    // Note: SignalFlushIfBackpressureRelieved is NOT called here. The trivial
    // branch consumed nothing, so backpressure cannot have relieved; the previous
    // tail-call inside PublishReaderState was wasted work and is now decoupled
    // (see §5.1). Skipping it also closes the only path by which the signaler
    // could run with a third-thread Complete in flight.
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
// value, including any ReaderCompleteFlag bit set by a third-thread Reader.Complete.
// The gate must be after TryAcquire, not at the top of the method — the fence is
// what makes the plain read correct. DO NOT hoist.
if ((_pipe._readAwaiter._state & PipelyAwaiter<ReadResult>.ReaderCompleteFlag) != 0)
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

// ... existing state mutation ...

_pipe.PublishReaderState();
_pipe.SignalFlushIfBackpressureRelieved();   // separated; was a tail-call inside PublishReaderState
```

## Section 5 — Interactions with existing invariants

### 5.1 — `PublishReaderState` / `SignalFlushIfBackpressureRelieved` decoupling + `Deliver*Result` completion-awareness

#### 5.1.1 — Why decouple

Today, `PublishReaderState` (Pipe.cs:351-366) calls `SignalFlushIfBackpressureRelieved` (Pipe.cs:374-404) as a tail. The latter is the only signaler that wakes the parked writer when *backpressure relieves* (as opposed to `SignalFlushAwaiterIfPending`, which is the unconditional signaler used by `Reader.Complete`).

The tail-coupling is what creates the original §5.1 problem (now repaired in this revision): `AdvanceTo`'s trivial branch — where the reader didn't actually consume anything — still ends up running `SignalFlushIfBackpressureRelieved`. In the third-thread `reader.Complete` scenario, that wake can race against the `Complete`-installed `ReaderCompleteFlag` and deliver a stale `FlushResult` to the parked writer.

The two functions are decoupled. Callers now invoke them explicitly:

- `AdvanceTo` **trivial branch**: calls only `PublishReaderState`. Skipping the signaler is *correct* (no consumption → no backpressure relief), not just expedient.
- `AdvanceTo` **non-trivial branch**: calls `PublishReaderState` then `SignalFlushIfBackpressureRelieved` explicitly. Gate before both.

Comment at Pipe.cs:387-388 updates from:

> Note: no `|| _reader.ReaderCompleted` clause — AdvanceTo's entry guard throws if `_reader.ReaderCompleted`, so this code path never runs post-completion. Reader.Complete uses SignalFlushAwaiterIfPending (unconditional).

to:

```
// Note: this function is now called only from AdvanceTo's non-trivial branch,
// after the gate at line 136 has thrown on any in-flight reader.Complete.
// The trivial branch deliberately does not call this (no consumption → no
// backpressure relief). Reader.Complete continues to use the unconditional
// SignalFlushAwaiterIfPending. See spec 2026-05-10-cross-thread-complete-design.md §5.1.
```

#### 5.1.2 — `DeliverFlushResult` / `DeliverReadResult` completion-awareness

`DeliverFlushResult` (Pipe.cs:424-431) currently builds its delivery from `_reader.LastPublishedReaderState`. Under the new design, that snapshot can be stale: a third-thread `reader.Complete` sets `ReaderCompleteFlag` and `_readerCompletion` but does not publish the terminal `ReaderState` (the deferred handler does, later, on the owner thread). A signaler firing in this window would otherwise hand the parked writer a `FlushResult(IsCompleted=false)`.

`DeliverFlushResult` is amended to consult `_readerCompletion` first:

```csharp
private void DeliverFlushResult()
{
    // _readerCompletion is set by Reader.Complete (any thread) via Interlocked.CompareExchange,
    // which is a full fence. All callers of DeliverFlushResult have just executed an
    // Interlocked op (either the signaler's Pending→Inactive CAS, or Reader.Complete's
    // Interlocked.Or on _flushAwaiter._state), so this plain read is post-fence and
    // observes the latest globally-visible value.
    var completion = _readerCompletion;

    if (completion is ExceptionDispatchInfo edi)
    {
        _flushAwaiter._core.SetException(edi.SourceException);
        return;
    }

    if (completion is not null)   // s_completedSuccessfully — graceful close
    {
        _flushAwaiter._core.SetResult(new FlushResult(isCanceled: false, isCompleted: true));
        return;
    }

    // Pre-completion: deliver from the published ReaderState as before.
    var r = _reader.LastPublishedReaderState;
    if (r.IsCompleted && r.CompletionException != null)
        _flushAwaiter._core.SetException(r.CompletionException);
    else
        _flushAwaiter._core.SetResult(new FlushResult(isCanceled: false, isCompleted: r.IsCompleted));
}
```

On the reader side there is no existing `DeliverReadResult` helper — the equivalent logic is inlined in `SignalReadAwaiterIfPending` (Pipe.cs:283-317, lines 295-313 are the construction block). As part of this change, factor that block out into a new `DeliverReadResult` helper and amend it symmetrically:

```csharp
private void DeliverReadResult()
{
    // _writerCompletion is set by Writer.Complete (any thread) via Interlocked.CompareExchange,
    // full fence. Callers of DeliverReadResult have just executed an Interlocked op
    // (either the signaler's Pending→Inactive CAS, or Writer.Complete's Interlocked.Or
    // on _readAwaiter._state), so this plain read is post-fence.
    var completion = _writerCompletion;

    if (completion is ExceptionDispatchInfo edi)
    {
        _readAwaiter._core.SetException(edi.SourceException);
        return;
    }

    // Pre-completion or graceful completion: build buffer from stash + last-published
    // writer state. If `completion` is s_completedSuccessfully, synthesize
    // IsCompleted=true (terminal state may not have been published yet by the
    // deferred handler).
    var w = _writer.LastPublishedWriterState;
    var head    = _readAwaiter._stashHead ?? w.HeadSegment;
    var headIdx = _readAwaiter._stashHead == null ? 0 : _readAwaiter._stashHeadIdx;
    var buffer = head == null
        ? ReadOnlySequence<byte>.Empty
        : new ReadOnlySequence<byte>(head, headIdx, w.TailSegment!, w.TailWritten);

    bool isCompleted = (completion is not null) || w.IsCompleted;
    _reader.ReadPending = true;
    _readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: false, isCompleted));
}
```

`SignalReadAwaiterIfPending` becomes thin — the Pending→Inactive CAS followed by `DeliverReadResult()`.

With both helpers in place, the parked-owner-wake guarantee matches BCL: a third-thread `Complete` wakes the parked peer with `IsCompleted=true` (graceful) or the rethrown `ExceptionDispatchInfo` (faulted), regardless of whether the deferred handler has run yet.

### 5.2 — `BuildReadResult` / `BuildFlushResult` consult `_completion`

Symmetric to §5.1.2 for the synchronous-return paths. `BuildReadResult` is called from `ReadAsync` / `TryRead` / `CancelPendingRead` to construct the synchronous `ReadResult`; `BuildFlushResult` from `FlushAsync`. Both consult `_writerCompletion` / `_readerCompletion` to synthesize `IsCompleted=true` when set, so peers see completion immediately on the synchronous path as well.

### 5.3 — `Pipe.Reset` / `Pipe.Dispose`

`_writerCompletion` and `_readerCompletion` are reset to `null` in `Pipe.Reset` so a recycled pipe starts fresh. `Pipe.Dispose` does the same.

`Pipe.Reset`'s existing precondition assertion (Pipe.cs:141, currently `if (!_writer.WriterCompleted || !_reader.ReaderCompleted) …`) is amended to also accept "completion requested but deferred handler hasn't run yet":

```csharp
if ((_writerCompletion is null && !_writer.WriterCompleted)
    || (_readerCompletion is null && !_reader.ReaderCompleted))
    throw new InvalidOperationException("Pipe.Reset requires both sides to have been Completed.");
```

Rationale: a caller who calls `Complete` then immediately calls `Reset` (without any intervening hot-path call) has not given the deferred handler a chance to run. The `_completion` field is non-null in that case, so the assertion accepts. (`Pipe.Reset` then also runs the deferred handlers as part of its cleanup to bring `LastPublished*State` and the `*Completed` bools into a consistent reset state, before zeroing everything.)

### 5.4 — `BufferedBytes` and other read-only accessors

`PipeWriter.BufferedBytes` (Pipe.Writer.cs:191-192) reads `LastAcquiredReaderState.TotalConsumed`. This continues to work — `TotalConsumed` is unaffected by completion.

`PipeWriter.UnflushedBytes` reads `LastPublishedWriterState.TotalWritten`. After self-`Complete`, this is the terminal `TotalWritten`. After third-thread `Complete` (deferred handler not yet run), this is the pre-completion `TotalWritten`. Both values are technically correct: "bytes written but not yet flushed" is well-defined regardless of completion state.

## Section 6 — Edge cases

### 6.1 — Concurrent `Complete` calls

Two threads (or the owner and a third thread) call `Complete` concurrently with different exceptions:

- Both compute their `captured` values and race on `Interlocked.CompareExchange(ref _completion, captured, null)`.
- One CAS returns null (winner installed its captured value); the other returns the winner's value (loser sees non-null) and silently returns.
- The winner sets its side's completion flag bit on both awaiter state words and signals.
- The loser's exception is discarded — matches BCL `TryComplete`.

Note: there is no requirement that the winner's flag bits and signals are observed by the loser; the loser exits before doing any further work.

### 6.2 — `Complete` called while owner is mid-`FlushAsync` / `ReadAsync` etc.

The owner-thread method is in some intermediate state (e.g., having just published a `WriterState` snapshot and about to park). Third thread calls `Complete`.

- Third thread's `Interlocked.Or` sets the relevant `*CompleteFlag` (`WriterCompleteFlag` for `Writer.Complete`, `ReaderCompleteFlag` for `Reader.Complete`).
- Third thread's `SignalFlushAwaiterIfPending` / `SignalReadAwaiterIfPending` either wakes a parked owner (if already Pending) or no-ops.
- Owner's continued execution: observation depends on the method:
  - **`FlushAsync` / `ReadAsync` / `TryRead`**: the next Interlocked op (the extended CAS drain) observes the flag and dispatches (self-throw or peer-completion delivery).
  - **`GetMemory` / `Advance` / `Splice`**: there is no Interlocked op in the body; observation is via the `Volatile.Read` gate (§4.1). The gate's acquire fence is sufficient to see the third-thread `Interlocked.Or` write.
  - **`AdvanceTo`**: observation is via the post-Interlocked plain read at the gate placed after `_writerTb.TryAcquire()` (non-trivial branch) or after `_pipe.PublishReaderState()` (trivial branch). Both fences are TripleBuffer Interlocked ops.
  - **Parked owner**: the `Park*Awaiter` lost-wakeup re-check observes the flag during its `TryAcquire`-fenced read sequence.

The signal-wakeup race is handled by the existing `PipelyAwaiter` state machine: the signal fails to find Pending (owner hasn't transitioned yet), but the owner's lost-wakeup re-check fires the throw on its own subsequent iteration. This is the same race shape that already exists for `CancelPending*`; the design is reused.

### 6.3 — `Complete` called multiple times on the owner thread

Standard idempotency. First call wins; second and subsequent return silently. Behavior identical to BCL.

### 6.4 — `Complete` called from a continuation of a `ReadAsync` / `FlushAsync`

The continuation runs on whichever thread the awaiter scheduled it on. If that's not the owner thread, it's a third-thread `Complete` from Pipely's perspective — the new contract handles it. If it's the owner thread (e.g., `RunContinuationsAsynchronously = false`, sync-completed task), it's a self-`Complete`. Both paths are valid and produce the same observable behavior.

### 6.5 — Reader observing writer-completion while writer is mid-flush

Third thread calls `writer.Complete()` while the writer thread is mid-`FlushAsync` (between `_writerTb.ProducerSlot()` write and `_writerTb.Publish()`). `WriterCompleteFlag` is set on `_readAwaiter._state`. The reader's next `ReadAsync` / `TryRead` drain catches it and either rethrows (faulted) or returns `IsCompleted=true` with whatever data was previously visible (graceful).

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

- `DeliverFlushResult_WithReaderCompletionSet_DeliversIsCompletedTrue` — directly exercise the third-thread-`reader.Complete`-wakes-parked-writer path; assert the awaited `FlushAsync` returns with `IsCompleted=true` (graceful) or rethrows `ex` (faulted), not `IsCompleted=false`. Symmetric `DeliverReadResult_WithWriterCompletionSet_DeliversIsCompletedTrue`.
- `AdvanceTo_TrivialBranch_DoesNotCallSignalFlushIfBackpressureRelieved` — pin the §5.1.1 decoupling. Use a mock / probe to verify the signaler isn't invoked when both `SequencePosition`s are default.
- `Reset_ClearsCompletionFields` — call `Complete`, then `Pipe.Reset`; assert `_writerCompletion` / `_readerCompletion` are null again and a fresh `Complete` cycle works.
- `Reset_AfterCompleteWithNoHotPathCall_Succeeds` — pin §5.3's amended precondition: `Complete` then immediately `Reset` (no intervening `GetMemory` / `ReadAsync` / etc.) must not throw `InvalidOperationException`.
- `BufferedBytes_AfterThirdThreadComplete_ReturnsConsistentValue` — pin the documented "TotalConsumed-based; insensitive to completion" property.

### 7.4 — Pre-existing test compatibility

All existing `Complete`-related tests (self-`Complete` paths) must continue to pass without modification. A regression run is part of the implementation plan's verification step.

## Section 8 — Out of scope

- Surfacing the completion exception via a separate observable (e.g., a `Task` representing the completion). Callers can already observe via the next `ReadAsync` / `FlushAsync`. Adding a separate observable would expand public API without addressing a documented use case.
- Cross-thread invocation of any other method (e.g., `GetMemory` from an arbitrary thread). The SPSC contract for everything except `Complete` and `CancelPending*` is preserved.
- Replacing the awaiter-state state machine with a different primitive. The bit-packed `_state` int is load-bearing for cancel-flag drains and now for complete-flag drains; expanding it isn't motivated by this change.
- Generalizing the `Publish` / `Signal` decoupling beyond `PublishReaderState` / `SignalFlushIfBackpressureRelieved`. A symmetric writer-side refactor of any analogous helpers can be done as needed during implementation but is not pre-required by this spec.
