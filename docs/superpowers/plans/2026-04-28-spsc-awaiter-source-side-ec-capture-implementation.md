# SpscAwaiter Source-Side EC Capture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `ExecutionContext` capture and continuation routing from the consumer-side flag-honoring path (current `ManualResetValueTaskSourceCore<T>` behavior) to the source-side `SpscAwaiter<T>`, following OpenTcp's `DispatchedValueTaskSource<T>` pattern. Closes Mechanism A (consumer-side `SynchronizationContext` / `TaskScheduler` capture silently overrode the dispatcher's chosen routing) and Mechanism B (when `FlowExecutionContext` was suppressed, `MRVTSC.SetResult` skipped `RunInternal` and the continuation ran under the dispatcher worker thread's drifted EC, leaking `AsyncLocal<T>` mutations across cycles) — both with one mechanism and zero source changes to existing `IContinuationDispatcher` implementations.

**Architecture:** Three new fields on `SpscAwaiter<T>` (`_realContinuation`, `_realState`, `_capturedEC`) populated in an overridden `OnCompleted` that captures EC on the consumer's thread and strips both `FlowExecutionContext` and `UseSchedulingContext` flags from the forwarded `_core.OnCompleted` call. Two new static delegates (`s_dispatch`, `s_invokeWithEc`) on `SpscAwaiter<T>`: `s_dispatch` is registered with `_core` and routes through the configured `IContinuationDispatcher`; `s_invokeWithEc` runs on the dispatcher's chosen thread and applies the captured EC via `ExecutionContext.Run`. Signal sites in `SpscPipe.cs` / `SpscPipe.Reader.cs` / `SpscPipe.Writer.cs` simplify from a stash-and-dispatch pattern to direct `_core.SetResult` / `_core.SetException` calls. The `_dispatchResult` / `_dispatchException` stash fields, the four `s_dispatch*` delegates, and the `DispatchVia` helper are deleted. Existing `IContinuationDispatcher` implementations require zero changes.

**Tech Stack:** C# / .NET 10, `Interlocked` primitives, `Volatile.Write` for publication ordering, `ExecutionContext.Capture` / `ExecutionContext.Run` for source-side EC handling, xUnit 2.9.3.

**Reference docs (engineer should re-read before starting):**
- `docs/superpowers/specs/2026-04-28-spsc-awaiter-source-side-ec-capture-design.md` — this plan's spec.
- `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md` — SpscPipe design; §5 awaiter coordination + continuation dispatch protocol; revised in Task 9.
- `docs/superpowers/specs/2026-04-27-hot-handoff-dispatcher-design.md` — HotHandoff dispatcher spec; §6 EC contract revised in Task 10.
- `docs/IContinuationDispatcher.md` — public-facing dispatcher contract; revised in Task 8.
- `src/SpscPipelines/SpscAwaiter.cs` — the type whose fields and methods this plan changes (Task 2).
- `src/SpscPipelines/SpscPipe.cs`, `src/SpscPipelines/SpscPipe.Reader.cs`, `src/SpscPipelines/SpscPipe.Writer.cs` — files containing the 12 signal sites and the 4 obsolete `s_dispatch*` delegates (Task 2).
- `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs` — existing dispatcher tests that establish the `DedicatedThreadDispatcher` / `ForwardingDispatcher` test helpers and the EC-flow test patterns; new tests in Tasks 2-7 append to this file.

**Working directory for all commands:** `/home/harrison/src/worktrees/SpscPipe/hot-handoff/`

**Spec/contract guarantees this implementation pins:**
- Per-await `ExecutionContext` propagates to the continuation, **provided `FlowExecutionContext` was set at `OnCompleted` time** (the default).
- Cross-cycle isolation: cycle N's mutations to an `AsyncLocal<T>` made inside the continuation do NOT leak into cycle N+1's continuation.
- `FlowExecutionContext` suppression results in NO `ExecutionContext.Run` (consumer explicitly opted out of EC propagation; the worker thread's current EC is what the continuation runs under).
- `SynchronizationContext` and `TaskScheduler` captured by the consumer's `await` are NOT honored — the continuation always runs on the dispatcher's chosen thread.
- The `SetResult`-fires-first race is closed by publication ordering: `_realContinuation` / `_realState` / `_capturedEC` are visibly written via `Volatile.Write` BEFORE `_core.OnCompleted` so the rare TP-dispatched `s_dispatch` reads them post-publication.

---

## Task 1: Pre-flight — revert TEMP HotHandoff benchmark commits

The spec explicitly puts this in the implementation plan rather than the design (§7 "Out of scope"). Three TEMP commits — `c1ba6b9`, `99293c1`, `02a6dd1` — replaced the throughput-shape benchmark workload with the latency-CLI MHz-rate workload while diagnosing Mechanism A. The original 1 MiB / 4 KiB-chunk / pre-allocated-byte-array workload must be restored before the new wiring can be measured against the established 50.45 µs / 70.34 µs / 105.09 µs HotHandoff / TpDefault / BCL baseline.

**Files:**
- Modify: `tests/SpscPipelines.HotHandoff.Benchmarks/DispatcherThroughputBench.cs`

- [ ] **Step 1: Restore the benchmark file from before the TEMP commits**

Run:

```bash
git checkout c1ba6b9^ -- tests/SpscPipelines.HotHandoff.Benchmarks/DispatcherThroughputBench.cs
```

Expected: file replaced with its pre-TEMP form.

- [ ] **Step 2: Verify the constants and shape**

Run:

```bash
grep -E 'TotalBytes|ChunkSize|chunk\.CopyTo|new byte\[ChunkSize\]' tests/SpscPipelines.HotHandoff.Benchmarks/DispatcherThroughputBench.cs
```

Expected output (any order):

```
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize  = 4096;
            var chunk = new byte[ChunkSize];
                chunk.CopyTo(memory);
```

- [ ] **Step 3: Build the solution to confirm clean state**

Run:

```bash
dotnet build SpscPipe.slnx
```

Expected: 6 projects build, 0 warnings, 0 errors.

- [ ] **Step 4: Commit**

```bash
git add tests/SpscPipelines.HotHandoff.Benchmarks/DispatcherThroughputBench.cs
git commit -m "$(cat <<'EOF'
HotHandoff bench: revert TEMP commits c1ba6b9 / 99293c1 / 02a6dd1

Restores the original 1 MiB / 4 KiB-chunk / pre-allocated-byte-array
throughput-shape workload. The TEMP commits replaced this with the
latency-CLI MHz-rate workload while diagnosing Mechanism A; the
diagnosis is now closed by the spec at
docs/superpowers/specs/2026-04-28-spsc-awaiter-source-side-ec-capture-design.md
and the throughput-shape benchmark must be in its original form for
the post-implementation measurement pass against the established
50.45 us / 70.34 us / 105.09 us HotHandoff / TpDefault / BCL baseline.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Test C.2 + source-side EC capture architecture (TDD)

The architecture-establishing task. Write test C.2 (cross-cycle `AsyncLocal` isolation — the Mechanism B regression test), watch it fail, then implement the full source-side EC capture per spec §2 in one cohesive change. Multiple files change together because the new wiring (s_dispatch + s_invokeWithEc + EC fields + signal-site simplification) is one architectural unit — adding the new without removing the old produces a double-dispatch / infinite-loop hazard, so they must change together.

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs`
- Modify: `src/SpscPipelines/SpscAwaiter.cs`
- Modify: `src/SpscPipelines/SpscPipe.cs`
- Modify: `src/SpscPipelines/SpscPipe.Reader.cs`
- Modify: `src/SpscPipelines/SpscPipe.Writer.cs`

- [ ] **Step 1: Write the failing test C.2**

Append this test method at the end of the `SpscPipeContinuationDispatcherTests` class in `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs` (just before the closing brace):

```csharp
// ---------- C.2 — Per-cycle EC capture/apply hygiene (regression-only) ----------

/// <summary>
/// Pins per-cycle EC capture/apply hygiene. Each await on the same SpscPipe goes
/// through SpscAwaiter.OnCompleted (capturing the consumer-thread EC at that
/// moment) followed by s_invokeWithEc on the dispatcher's chosen thread (which
/// reads, applies via ExecutionContext.Run, AND clears _realContinuation /
/// _realState / _capturedEC). If the field clearing in s_invokeWithEc were ever
/// removed or reordered, cycle 2 might observe stale field state from cycle 1 —
/// e.g., run under cycle 1's captured EC instead of its own. The test exercises
/// two consecutive awaits with different consumer-side AsyncLocal values and
/// asserts cycle 2's continuation observes cycle 2's value.
///
/// Note: this test does NOT differentiate the new wiring from the old wiring.
/// Under the old wiring, MRVTSC.RunInternal scoped each cycle's captured EC
/// equivalently, and the observable outcome is the same. The test's value is
/// regression protection going forward against accidental removal of the
/// per-cycle field reset in s_invokeWithEc; it is NOT a Mechanism B reproducer.
/// (Mechanism B's leak structurally requires SuppressFlow on both the prior
/// and current cycles, and is structurally identical in both wirings — neither
/// fixes the SuppressFlow-on-both case. The new wiring's value is mostly
/// architectural cleanliness plus the Mechanism A scheduler-bypass fix.)
/// </summary>
[Fact]
public async Task MultiCycle_PerCycleEcCapture_AppliesCorrectEcEachCycle()
{
    var asyncLocal = new AsyncLocal<int>();
    using var dispatcher = new DedicatedThreadDispatcher();
    using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

    asyncLocal.Value = 42;

    // Cycle 1: producer fires after consumer parks; consumer's continuation
    // mutates asyncLocal to 999. The mutation is scoped to the cycle's EC frame
    // (ExecutionContext.Run in new wiring; MRVTSC.RunInternal in old) — does NOT
    // drift onto the dispatcher's worker thread; both wirings restore the worker's
    // pre-cb EC after the continuation returns.
    _ = Task.Run(async () =>
    {
        await Task.Delay(50);
        var mem = pipe.Writer.GetMemory(5);
        mem.Span.Clear();
        pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();
    });

    var rr1 = await pipe.Reader.ReadAsync();
    pipe.Reader.AdvanceTo(rr1.Buffer.End);
    asyncLocal.Value = 999;     // mutation inside cycle-1's EC scope (does not drift onto worker)

    // Cycle 2: a fresh await on the same pipe. The expected continuation observation
    // is whatever the consumer's calling-site EC has at OnCompleted time. We set
    // it to 7 here. If cycle 2's captured EC were ever stale (e.g., s_invokeWithEc
    // failed to clear _capturedEC between cycles), cycle 2 might run under cycle 1's
    // captured EC (asyncLocal=42). The assertion below catches that regression.
    asyncLocal.Value = 7;
    _ = Task.Run(async () =>
    {
        await Task.Delay(50);
        var mem = pipe.Writer.GetMemory(3);
        mem.Span.Clear();
        pipe.Writer.Advance(3);
        await pipe.Writer.FlushAsync();
    });

    var rr2 = await pipe.Reader.ReadAsync();
    pipe.Reader.AdvanceTo(rr2.Buffer.End);

    Assert.Equal(3, rr2.Buffer.Length);
    Assert.Equal(7, asyncLocal.Value);
}
```

- [ ] **Step 2: Run the test (regression-only baseline)**

Run:

```bash
dotnet test tests/SpscPipe.Tests --nologo --filter MultiCycle_PerCycleEcCapture_AppliesCorrectEcEachCycle
```

Expected: PASS. Under the old wiring, `MRVTSC.RunInternal` scopes each cycle's captured EC equivalently to the new wiring; the test pins per-cycle EC capture/apply as a forward regression guard, not a mechanism reproducer. The test must continue to PASS after the architecture switch in subsequent steps.

(Do not commit yet — the implementation arrives in subsequent steps.)

- [ ] **Step 3: Replace `src/SpscPipelines/SpscAwaiter.cs` with the new EC-capture form**

Write to `src/SpscPipelines/SpscAwaiter.cs` (overwriting):

```csharp
using System.Threading;
using System.Threading.Tasks.Sources;

namespace SpscPipelines;

internal sealed class SpscAwaiter<T> : IValueTaskSource<T>
{
    // RCA = false: with source-side EC capture, every signal-path SetResult/SetException
    // invokes our registered s_dispatch INLINE on the producer thread (RCA=false ⇒ MRVTSC
    // runs the registered callback synchronously on the calling thread). s_dispatch then
    // routes the work item through the configured IContinuationDispatcher. With RCA=true,
    // MRVTSC would queue s_dispatch to the ThreadPool itself before invoking it — adding
    // a redundant TP hop and breaking the dispatcher's thread-routing guarantee. See spec
    // §2.5. RCA=false is set once at construction; ManualResetValueTaskSourceCore<T>.Reset
    // does NOT reset this flag, so it remains correct across park cycles.
    public ManualResetValueTaskSourceCore<T> _core = new() { RunContinuationsAsynchronously = false };
    public int _state;
    public CancellationTokenRegistration _ctr;
    public CancellationToken _token;

    // Pattern 2 stash (used by SpscAwaiter<ReadResult>; ignored by SpscAwaiter<FlushResult>).
    public BufferSegment? _stashHead;
    public int _stashHeadIdx;
    public BufferSegment? _stashTail;
    public int _stashTailIdx;

    // Source-side EC-capture stash. Written by OnCompleted on the consumer's thread BEFORE
    // delegating to _core.OnCompleted (so visible by the time s_dispatch reads them — including
    // in the SetResult-fires-first race; see spec §4 publication ordering). Read by
    // s_invokeWithEc on the dispatcher's chosen thread, which applies _capturedEC via
    // ExecutionContext.Run if non-null and invokes _realContinuation(_realState).
    public Action<object?>? _realContinuation;
    public object? _realState;
    public ExecutionContext? _capturedEC;

    // Worker-thread-only scratch fields used by the allocation-free ExecutionContext.Run pattern
    // (spec §5). s_invokeWithEc writes _runCb/_runState before ExecutionContext.Run; s_runContinuation
    // reads them and clears them. Only the dispatcher's chosen thread accesses these, sequentially
    // around each Run invocation, so plain reads/writes are sufficient — no concurrent writers.
    private Action<object?>? _runCb;
    private object? _runState;

    // The dispatcher this awaiter routes continuations through. Set once at construction;
    // immutable for the awaiter's lifetime. Stored on the awaiter so s_dispatch can reach it
    // without a back-pointer to SpscPipe.
    private readonly IContinuationDispatcher _dispatcher;

    public const int Inactive   = 0b00;
    public const int Pending    = 0b01;
    public const int StateMask  = 0b01;
    public const int CancelFlag = 0b10;

    // Diagnostic counters (Interlocked-incremented at each CAS resolution site). Cost ~5-10 ns
    // per increment, only on park/signal paths (off the synchronous hot path). Read by the
    // benchmark project (`tests/SpscPipe.Benchmarks/SpscPipeAdapter.cs`) after a run completes;
    // unrelated to the EC-capture work.
    public long _parkCount;
    public long _signalWonCount;
    public long _tokenCancelWonCount;
    public long _cancelPendingWonCount;
    public long _lostWakeupResolvedCount;
    public long _lostCancelResolvedCount;

    public SpscAwaiter(IContinuationDispatcher dispatcher) => _dispatcher = dispatcher;

    public short Version => _core.Version;
    public T GetResult(short token) => _core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(
        Action<object?> continuation, object? state,
        short token, ValueTaskSourceOnCompletedFlags flags)
    {
        // Capture EC on the awaiter thread (the consumer's), before MRVTSC's barrier. Capturing
        // inside s_dispatch instead would get the producer thread's EC in the SetResult-fires-first
        // race — wrong; would silently leak AsyncLocal<T> values across requests. See spec §2.2.
        ExecutionContext? ec =
            (flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0
                ? ExecutionContext.Capture()
                : null;

        // Volatile.Write publication order: cap EC, then state, then continuation. The TP-dispatched
        // s_dispatch in the SetResult-fires-first race reads these post-publication via the
        // happens-before edge from queue-call to dequeued callback. See spec §4.
        Volatile.Write(ref _capturedEC, ec);
        Volatile.Write(ref _realState, state);
        Volatile.Write(ref _realContinuation, continuation);

        // Strip both EC and SchedulingContext flags before forwarding. EC is captured by us;
        // leaving the flag on would have MRVTSC capture again (wasteful, unused). SchedulingContext
        // is stripped to honor the dispatcher's contract — the dispatcher controls routing,
        // not the consumer's captured SC/TaskScheduler. See spec §2.2 and §3.3.
        const ValueTaskSourceOnCompletedFlags suppressed =
            ValueTaskSourceOnCompletedFlags.FlowExecutionContext |
            ValueTaskSourceOnCompletedFlags.UseSchedulingContext;
        _core.OnCompleted(s_dispatch, this, token, flags & ~suppressed);
    }

    // Registered with _core via OnCompleted. Invoked inline by MRVTSC.SetResult on the producer
    // thread (RCA=false), or — in the rare SetResult-fires-first race — queued to TP by MRVTSC
    // and invoked there. In either case, routes the work item (the awaiter itself, as state)
    // through the dispatcher.
    private static readonly Action<object?> s_dispatch = static state =>
    {
        var awaiter = (SpscAwaiter<T>)state!;
        awaiter._dispatcher.UnsafeQueueUserWorkItem(s_invokeWithEc, awaiter);
    };

    // Invoked by the dispatcher's chosen thread (HotHandoff worker, TP worker for overflow, or
    // TP for ThreadPoolContinuationDispatcher). Reads the awaiter's fields, clears them, applies
    // the consumer-captured EC if any, and invokes the continuation.
    private static readonly Action<object?> s_invokeWithEc = static state =>
    {
        var awaiter = (SpscAwaiter<T>)state!;
        var cont = awaiter._realContinuation;
        var st   = awaiter._realState;
        var ec   = awaiter._capturedEC;
        awaiter._realContinuation = null;
        awaiter._realState = null;
        awaiter._capturedEC = null;

        if (ec is not null)
        {
            // Allocation-free pattern (spec §5): pass the awaiter as state to ExecutionContext.Run,
            // staging cont/st via worker-thread-only scratch fields. The static ContextCallback
            // reads cont/st and clears the scratch fields on the worker thread.
            awaiter._runCb = cont;
            awaiter._runState = st;
            ExecutionContext.Run(ec, s_runContinuation, awaiter);
        }
        else
        {
            // Consumer suppressed FlowExecutionContext at OnCompleted time — explicitly opted
            // out of EC propagation. Invoke under the dispatcher's chosen thread's current EC;
            // no capture/apply.
            cont!(st);
        }
    };

    // ContextCallback wrapper used by ExecutionContext.Run inside s_invokeWithEc.
    private static readonly ContextCallback s_runContinuation = static state =>
    {
        var awaiter = (SpscAwaiter<T>)state!;
        var cb = awaiter._runCb!;
        var st = awaiter._runState;
        awaiter._runCb = null;
        awaiter._runState = null;
        cb(st);
    };
}
```

- [ ] **Step 4: Replace `src/SpscPipelines/SpscPipe.cs` with the simplified form**

Write to `src/SpscPipelines/SpscPipe.cs` (overwriting):

```csharp
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;

namespace SpscPipelines;

public sealed partial class SpscPipe : IDisposable
{
    internal readonly SpscPipeOptions _options;
    internal readonly TripleBuffer<WriterState> _writerTb = new();
    internal readonly TripleBuffer<ReaderState> _readerTb = new();
    internal readonly SpscAwaiter<ReadResult>  _readAwaiter;
    internal readonly SpscAwaiter<FlushResult> _flushAwaiter;

    // Writer-side cursors (writer thread only).
    internal BufferSegment? _chainHead;
    internal BufferSegment? _writingHead;
    internal int  _writingHeadBytesBuffered;
    internal long _totalWritten;
    internal BufferSegment? _freelistHead;
    internal int  _freelistCount;
    internal WriterState _lastPublishedWriterState;
    internal ReaderState _lastAcquiredReaderState;
    internal bool _writerCompleted;

    // Reader-side cursors (reader thread only).
    internal BufferSegment? _readHead;
    internal int _readHeadIdx;
    internal BufferSegment? _readTail;
    internal int _readTailIdx;
    internal long _totalConsumed;
    internal long _totalExamined;
    internal ReaderState _lastPublishedReaderState;
    internal WriterState _lastAcquiredWriterState;
    internal bool _readerCompleted;
    // True when a ReadResult has been delivered to the user but not yet AdvanceTo'd.
    // Set on every ReadResult-delivery site (sync return + park SetResult); cleared in AdvanceTo.
    // Cross-thread sets (signaler, canceler) ride on _core's SetResult/await synchronization edge.
    internal bool _readPending;

    // Pipe-level (mutated by Dispose only).
    internal bool _disposed;

    private readonly SpscPipeWriter _writerInstance;
    private readonly SpscPipeReader _readerInstance;

    public SpscPipe() : this(SpscPipeOptions.Default) { }
    public SpscPipe(SpscPipeOptions options)
    {
        _options = options;
        var dispatcher = options.ContinuationDispatcher ?? ThreadPoolContinuationDispatcher.Instance;
        _readAwaiter    = new SpscAwaiter<ReadResult>(dispatcher);
        _flushAwaiter   = new SpscAwaiter<FlushResult>(dispatcher);
        _writerInstance = new SpscPipeWriter(this);
        _readerInstance = new SpscPipeReader(this);
    }

    public PipeWriter Writer => _writerInstance;
    public PipeReader Reader => _readerInstance;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // R4-1: dispose leftover CTRs (idempotent on default).
        _readAwaiter._ctr.Dispose();
        _flushAwaiter._ctr.Dispose();

        // Walk the chain.
        var seg = _chainHead;
        while (seg != null)
        {
            var next = seg.Next;
            seg.DisposeOwned();
            seg = next;
        }
        _chainHead = null;
        _writingHead = null;

        // Walk the freelist.
        var fl = _freelistHead;
        while (fl != null)
        {
            var next = fl.Next;
            fl.DisposeOwned();
            fl = next;
        }
        _freelistHead = null;
        _freelistCount = 0;
    }

    internal BufferSegment RentSegment(int sizeHint, long runningIndex)
    {
        var s = PopFreelist(minSize: sizeHint);
        if (s != null)
        {
            s.RecycleReset(runningIndex);
            return s;
        }
        s = new BufferSegment();
        s.RentFrom(_options.Pool, Math.Max(sizeHint, _options.MinimumSegmentSize), runningIndex, owner: this);
        return s;
    }

    private BufferSegment? PopFreelist(int minSize)
    {
        var head = _freelistHead;
        if (head == null) return null;
        if (head.AvailableMemory.Length < minSize)
        {
            // Drop and dispose; per Spec §3 N5 (avoid stranding small segments).
            _freelistHead = head.Next;
            head.DisposeOwned();
            _freelistCount--;
            return null;
        }
        _freelistHead = head.Next;
        _freelistCount--;
        // Don't RecycleReset here — RentSegment does it with the correct runningIndex,
        // which also clears the freelist-link Next set by PushFreelist.
        return head;
    }

    internal void PushFreelist(BufferSegment s)
    {
        if (_freelistCount >= _options.MaxFreelistSegments)
        {
            s.DisposeOwned();
            return;
        }
        // Reset to clean state, then link into the freelist via SetFreelistNext.
        s.RecycleReset(runningIndex: 0);
        s.SetFreelistNext(_freelistHead);
        _freelistHead = s;
        _freelistCount++;
    }

    internal bool HasReadableProgress() => _lastAcquiredWriterState.TotalWritten > _totalExamined;

    internal void IntegrateAcquiredWriterState()
    {
        var w = _lastAcquiredWriterState;
        if (_readHead == null)                  // I10 bootstrap
        {
            _readHead    = w.HeadSegment;
            _readHeadIdx = 0;
        }
        _readTail    = w.TailSegment;
        _readTailIdx = w.TailWritten;
    }

    internal ReadResult BuildReadResult(bool isCanceled)
    {
        bool isCompleted = _lastAcquiredWriterState.IsCompleted;
        var buffer = _readHead == null
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(_readHead, _readHeadIdx, _readTail!, _readTailIdx);
        return new ReadResult(buffer, isCanceled, isCompleted);
    }

    internal FlushResult BuildFlushResult(bool isCanceled)
        => new(isCanceled, isCompleted: _lastAcquiredReaderState.IsCompleted);

    internal void SignalReadAwaiterIfPending()
    {
        while (true)
        {
            int oldV = _readAwaiter._state;
            if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) return;
            int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
            if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _readAwaiter._signalWonCount);
                _readAwaiter._ctr.Dispose();

                // Pattern 2: construct ReadResult from stash + just-published WriterState.
                var w = _lastPublishedWriterState;

                // Throw-first: writer-completed-with-ex delivered as exception.
                if (w.IsCompleted && w.CompletionException != null)
                {
                    _readAwaiter._core.SetException(w.CompletionException);
                    return;
                }

                var head    = _readAwaiter._stashHead ?? w.HeadSegment;     // bootstrap fallback
                var headIdx = _readAwaiter._stashHead == null ? 0 : _readAwaiter._stashHeadIdx;

                var buffer = head == null
                    ? ReadOnlySequence<byte>.Empty
                    : new ReadOnlySequence<byte>(head, headIdx, w.TailSegment!, w.TailWritten);

                _readPending = true;
                _readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: false, isCompleted: w.IsCompleted));
                return;
            }
        }
    }

    internal void OnReadAwaiterTokenCancel()
    {
        while (true)
        {
            int oldV = _readAwaiter._state;
            if ((oldV & SpscAwaiter<ReadResult>.StateMask) != SpscAwaiter<ReadResult>.Pending) return;
            int desired = oldV & ~SpscAwaiter<ReadResult>.StateMask;
            if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _readAwaiter._tokenCancelWonCount);
                _readAwaiter._core.SetException(new OperationCanceledException(_readAwaiter._token));
                return;
            }
        }
    }

    internal void OnFlushAwaiterTokenCancel()
    {
        while (true)
        {
            int oldV = _flushAwaiter._state;
            if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) return;
            int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
            if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _flushAwaiter._tokenCancelWonCount);
                _flushAwaiter._core.SetException(new OperationCanceledException(_flushAwaiter._token));
                return;
            }
        }
    }

    internal void PublishReaderState()
    {
        var snapshot = new ReaderState
        {
            HeadSegment         = _readHead,
            TotalConsumed       = _totalConsumed,
            TotalExamined       = _totalExamined,
            IsCompleted         = false,
            CompletionException = null,
        };
        _readerTb.ProducerSlot() = snapshot;
        _readerTb.Publish();
        _lastPublishedReaderState = snapshot;

        SignalFlushIfBackpressureRelieved();
    }

    // R2-1: gated signaler — only wakes the parked writer when backpressure has relieved.
    // Called from AdvanceTo. The writer cannot re-check the wake condition after _core.SetResult,
    // so signal-side gating is required (not optional).
    //
    // Side effect: the inner TryAcquire mutates _readTail/_readTailIdx via IntegrateAcquiredWriterState.
    // Benign — keeps reader's view of the writer's tail fresh as a no-op-or-better.
    internal void SignalFlushIfBackpressureRelieved()
    {
        // Fast path: no parked writer.
        if ((_flushAwaiter._state & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) return;

        // Refresh writer state to compute unconsumed accurately.
        if (_writerTb.TryAcquire())
        {
            _lastAcquiredWriterState = _writerTb.ConsumerSlot();
            IntegrateAcquiredWriterState();
        }

        long unconsumed = _lastAcquiredWriterState.TotalWritten - _totalConsumed;
        // Note: no `|| _readerCompleted` clause — AdvanceTo's entry guard throws if _readerCompleted,
        // so this code path never runs post-completion. Reader.Complete uses SignalFlushAwaiterIfPending (unconditional).
        if (unconsumed >= _options.ResumeWriterThreshold) return;

        while (true)
        {
            int oldV = _flushAwaiter._state;
            if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) return;
            int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
            if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _flushAwaiter._signalWonCount);
                _flushAwaiter._ctr.Dispose();
                DeliverFlushResult();
                return;
            }
        }
    }

    // Unconditional signaler — used by Reader.Complete only (completion is always a wake reason).
    internal void SignalFlushAwaiterIfPending()
    {
        while (true)
        {
            int oldV = _flushAwaiter._state;
            if ((oldV & SpscAwaiter<FlushResult>.StateMask) != SpscAwaiter<FlushResult>.Pending) return;
            int desired = oldV & ~SpscAwaiter<FlushResult>.StateMask;
            if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _flushAwaiter._signalWonCount);
                _flushAwaiter._ctr.Dispose();
                DeliverFlushResult();
                return;
            }
        }
    }

    private void DeliverFlushResult()
    {
        var r = _lastPublishedReaderState;
        if (r.IsCompleted && r.CompletionException != null)
            _flushAwaiter._core.SetException(r.CompletionException);
        else
            _flushAwaiter._core.SetResult(new FlushResult(isCanceled: false, isCompleted: r.IsCompleted));
    }

    internal void RecycleDrainedSegments()
    {
        var r = _lastAcquiredReaderState;
        if (r.HeadSegment is null && !r.IsCompleted) return;       // pre-bootstrap

        var readerHead = r.HeadSegment;

        while (_chainHead != _writingHead && _chainHead != readerHead)
        {
            var recycled = _chainHead!;
            _chainHead   = recycled.Next!;
            PushFreelist(recycled);
        }
    }
}
```

Note the deletions vs the prior file:
- `s_dispatchReadSetResult`, `s_dispatchReadSetException`, `s_dispatchFlushSetResult`, `s_dispatchFlushSetException` static delegates — REMOVED (replaced by `s_dispatch` and `s_invokeWithEc` in `SpscAwaiter<T>`).
- `internal void DispatchVia(Action<object?>, object?)` helper — REMOVED.
- Awaiter ctor lines went from `new SpscAwaiter<ReadResult>()` to `new SpscAwaiter<ReadResult>(dispatcher)`.
- Every signal-site `_xxxAwaiter._dispatchResult = ...; DispatchVia(s_dispatchXxx, _xxxAwaiter);` collapsed to a single `_xxxAwaiter._core.SetResult(...);` (or `SetException`).

- [ ] **Step 5: Edit `src/SpscPipelines/SpscPipe.Reader.cs` — convert the three signal sites**

Apply three edits to `src/SpscPipelines/SpscPipe.Reader.cs`:

**Edit 1: `CancelPendingRead`** — find and replace this block:

```csharp
                Interlocked.Increment(ref _pipe._readAwaiter._cancelPendingWonCount);
                _pipe._readPending = true;
                _pipe._readAwaiter._dispatchResult = new ReadResult(buffer, isCanceled: true, isCompleted: false);
                _pipe.DispatchVia(s_dispatchReadSetResult, _pipe._readAwaiter);
```

with:

```csharp
                Interlocked.Increment(ref _pipe._readAwaiter._cancelPendingWonCount);
                _pipe._readPending = true;
                _pipe._readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: true, isCompleted: false));
```

**Edit 2: `ParkReadAwaiter` lost-wakeup writer-completion-exception path** — find and replace:

```csharp
                            Interlocked.Increment(ref _pipe._readAwaiter._lostWakeupResolvedCount);
                            _pipe._readAwaiter._dispatchException = _pipe._lastAcquiredWriterState.CompletionException;
                            _pipe.DispatchVia(s_dispatchReadSetException, _pipe._readAwaiter);
                            return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
```

with:

```csharp
                            Interlocked.Increment(ref _pipe._readAwaiter._lostWakeupResolvedCount);
                            _pipe._readAwaiter._core.SetException(_pipe._lastAcquiredWriterState.CompletionException);
                            return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
```

**Edit 3: `ParkReadAwaiter` lost-cancel re-check path** — find and replace:

```csharp
                Interlocked.Increment(ref _pipe._readAwaiter._lostCancelResolvedCount);
                _pipe._readPending = true;
                _pipe._readAwaiter._dispatchResult = _pipe.BuildReadResult(isCanceled: true);
                _pipe.DispatchVia(s_dispatchReadSetResult, _pipe._readAwaiter);
                return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
```

with:

```csharp
                Interlocked.Increment(ref _pipe._readAwaiter._lostCancelResolvedCount);
                _pipe._readPending = true;
                _pipe._readAwaiter._core.SetResult(_pipe.BuildReadResult(isCanceled: true));
                return new ValueTask<ReadResult>(_pipe._readAwaiter, _pipe._readAwaiter.Version);
```

- [ ] **Step 6: Edit `src/SpscPipelines/SpscPipe.Writer.cs` — convert the three signal sites**

Apply three edits to `src/SpscPipelines/SpscPipe.Writer.cs`:

**Edit 1: `CancelPendingFlush`** — find and replace:

```csharp
                Interlocked.Increment(ref _pipe._flushAwaiter._cancelPendingWonCount);
                _pipe._flushAwaiter._ctr.Dispose();
                _pipe._flushAwaiter._dispatchResult = new FlushResult(isCanceled: true, isCompleted: false);
                _pipe.DispatchVia(s_dispatchFlushSetResult, _pipe._flushAwaiter);
```

with:

```csharp
                Interlocked.Increment(ref _pipe._flushAwaiter._cancelPendingWonCount);
                _pipe._flushAwaiter._ctr.Dispose();
                _pipe._flushAwaiter._core.SetResult(new FlushResult(isCanceled: true, isCompleted: false));
```

**Edit 2: `ParkFlushAwaiter` lost-wakeup reader-completion-exception path** — find and replace:

```csharp
                            Interlocked.Increment(ref _pipe._flushAwaiter._lostWakeupResolvedCount);
                            _pipe._flushAwaiter._dispatchException = _pipe._lastAcquiredReaderState.CompletionException;
                            _pipe.DispatchVia(s_dispatchFlushSetException, _pipe._flushAwaiter);
                            return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
```

with:

```csharp
                            Interlocked.Increment(ref _pipe._flushAwaiter._lostWakeupResolvedCount);
                            _pipe._flushAwaiter._core.SetException(_pipe._lastAcquiredReaderState.CompletionException);
                            return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
```

**Edit 3: `ParkFlushAwaiter` lost-cancel re-check path** — find and replace:

```csharp
                Interlocked.Increment(ref _pipe._flushAwaiter._lostCancelResolvedCount);
                _pipe._flushAwaiter._dispatchResult = _pipe.BuildFlushResult(isCanceled: true);
                _pipe.DispatchVia(s_dispatchFlushSetResult, _pipe._flushAwaiter);
                return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
```

with:

```csharp
                Interlocked.Increment(ref _pipe._flushAwaiter._lostCancelResolvedCount);
                _pipe._flushAwaiter._core.SetResult(_pipe.BuildFlushResult(isCanceled: true));
                return new ValueTask<FlushResult>(_pipe._flushAwaiter, _pipe._flushAwaiter.Version);
```

- [ ] **Step 7: Build the whole solution**

Run:

```bash
dotnet build SpscPipe.slnx
```

Expected: 6 projects build, 0 warnings, 0 errors.

If the build fails with `_dispatchResult` / `_dispatchException` / `s_dispatchReadSetResult` / `s_dispatchReadSetException` / `s_dispatchFlushSetResult` / `s_dispatchFlushSetException` / `DispatchVia` not found, locate the leftover reference in Reader.cs / Writer.cs and apply the edit pattern from steps 5-6 (one line of stash + one line of `DispatchVia` collapses to one line of `_core.SetResult` or `_core.SetException`).

If `tests/SpscPipe.Tests/SpscAwaiterTests.cs` fails to compile because its `new SpscAwaiter<int>()` no longer matches the new `(IContinuationDispatcher)` constructor, **fix that test file in this same task** by changing every `new SpscAwaiter<int>()` to `new SpscAwaiter<int>(ThreadPoolContinuationDispatcher.Instance)`. Add a comment if helpful: `// Test-only — uses the default singleton dispatcher.` `ThreadPoolContinuationDispatcher` is `internal`, so the test project must already have access; if not, an `[InternalsVisibleTo("SpscPipe.Tests")]` attribute may be needed in `src/SpscPipelines/SpscPipelines.csproj`. Check first whether `SpscAwaiterTests.cs` references any field-level signature changes — read the file before editing.

- [ ] **Step 8: Verify `SpscAwaiterTests.cs` compiles cleanly**

Read `tests/SpscPipe.Tests/SpscAwaiterTests.cs`. Locate every `new SpscAwaiter<int>()`. Replace each with `new SpscAwaiter<int>(ThreadPoolContinuationDispatcher.Instance)` (the unit tests don't actually exercise dispatch, they only test the field/state machine — passing the singleton TP dispatcher is harmless).

If `ThreadPoolContinuationDispatcher` is internal and inaccessible, add an `InternalsVisibleTo` element to `src/SpscPipelines/SpscPipelines.csproj`:

```xml
<ItemGroup>
  <InternalsVisibleTo Include="SpscPipe.Tests" />
</ItemGroup>
```

(Only add this if the build fails on inaccessibility — the existing test project already accesses `SpscAwaiter<T>` which is also internal, so the attribute may already exist or may not be needed.)

Run:

```bash
dotnet build SpscPipe.slnx
```

Expected: clean.

- [ ] **Step 9: Update `CustomDispatcher_BadImpl_CapturingEC_IsDetectable` docstring (rationale shift under new wiring)**

The existing test `CustomDispatcher_BadImpl_CapturingEC_IsDetectable` in `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs` continues to pass under the new wiring, but its inline rationale comments still refer to `MRVTSC.RunInternal` as the mechanism that protects the consumer from the bad dispatcher's EC capture. That mechanism is no longer in play — under the new wiring, `SpscAwaiter.OnCompleted` captures the consumer's EC on the consumer's thread *before* the bad dispatcher ever sees the work item; `s_invokeWithEc` applies that captured EC regardless of what EC the bad dispatcher captured.

Same observable assertions, different reason. Update the inline comments and (if absent) add an XML doc summary so a future reader doesn't think the test passes for the old reason.

Apply this edit to `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs`:

Find the test method (currently has no XML doc summary, just the `[Fact]` attribute):

```csharp
    [Fact]
    public async Task CustomDispatcher_BadImpl_CapturingEC_IsDetectable()
```

Replace with:

```csharp
    /// <summary>
    /// Pins that a "bad" dispatcher (one that captures EC at queue time, e.g.,
    /// uses ThreadPool.QueueUserWorkItem instead of UnsafeQueueUserWorkItem) does
    /// NOT corrupt the consumer's continuation EC. Under the new source-side EC
    /// capture wiring, the consumer's EC is captured by SpscAwaiter.OnCompleted
    /// on the CONSUMER's thread — BEFORE the bad dispatcher ever sees the work
    /// item. s_invokeWithEc applies the source-side-captured EC via
    /// ExecutionContext.Run, regardless of what EC the bad dispatcher captured
    /// in its UnsafeQueueUserWorkItem. The bad dispatcher's capture is wasted
    /// work but does not break the consumer's continuation.
    ///
    /// (Historical note: under the OLD wiring, the protection came from
    /// MRVTSC.RunInternal applying the consumer's captured EC at SetResult time.
    /// Same observable assertions; different mechanism.)
    /// </summary>
    [Fact]
    public async Task CustomDispatcher_BadImpl_CapturingEC_IsDetectable()
```

Then within the test body, locate the closing comment block before `Assert.Equal(0, observedProducer);`:

```csharp
        // The producer's EC, if captured by the bad dispatcher, would be applied to
        // the work item BEFORE the inner RunInternal restoration. The continuation
        // sees consumer's EC during execution due to RunInternal — so producer's
        // value is NOT visible. This test documents the safety property: even a
        // dispatcher that captures EC doesn't break the consumer's continuation.
        Assert.Equal(0, observedProducer);
```

Replace with:

```csharp
        // The producer's EC, if captured by the bad dispatcher, never reaches the
        // continuation: SpscAwaiter.OnCompleted already captured the consumer's
        // EC on the consumer's thread BEFORE the bad dispatcher's queue-time
        // capture could matter, and s_invokeWithEc applies that captured consumer
        // EC via ExecutionContext.Run on the dispatcher's chosen thread. The bad
        // dispatcher's EC capture is wasted work, not a correctness hazard.
        Assert.Equal(0, observedProducer);
```

(Build to confirm the file still compiles; the test should still pass.)

Run:

```bash
dotnet build SpscPipe.slnx
```

Expected: clean.

- [ ] **Step 10: Run the full test suite**

Run:

```bash
dotnet test SpscPipe.slnx --nologo
```

Expected: ALL tests pass, including:
- The new `MultiCycle_PerCycleEcCapture_AppliesCorrectEcEachCycle` from step 1.
- The existing `CustomDispatcher_AsyncLocalFlowsToContinuation`, `CustomDispatcher_DispatcherThreadAsyncLocal_NotObservedInContinuation`, `CustomDispatcher_DispatcherThreadAsyncLocal_RestoredAfterContinuation`, `CustomDispatcher_BadImpl_CapturingEC_IsDetectable` (the last one with its docstring updated in step 9 to reflect the new mechanism).
- The HotHandoff tests including the `RepeatedIteration_PerMessageConsumer_DoesNotHang` BDN-pattern stress test — which was the live regression smoke for Mechanism A.
- All `BclParityTests`, `SpscPipeAdvanceToTests`, `SpscPipeCancellationTests`, `SpscPipeDisposeTests`, `SpscPipeLifecycleTests`, `SpscPipeReaderTests`, `SpscPipeReadInProgressTests`, `SpscPipeWriterTests`, `SpscAwaiterTests`, `BufferSegmentTests`.

If a test fails, do NOT proceed — diagnose. The existing tests pin observable behavior; a failure means the new wiring broke the contract somewhere.

- [ ] **Step 11: Commit**

```bash
git add src/SpscPipelines/SpscAwaiter.cs src/SpscPipelines/SpscPipe.cs src/SpscPipelines/SpscPipe.Reader.cs src/SpscPipelines/SpscPipe.Writer.cs tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs tests/SpscPipe.Tests/SpscAwaiterTests.cs
git commit -m "$(cat <<'EOF'
SpscAwaiter: source-side EC capture (OpenTcp pattern)

ExecutionContext capture and continuation routing move from the
consumer-side flag-honoring path (current MRVTSC behavior) to the
source-side SpscAwaiter<T>. Closes Mechanism A (consumer-captured
SC/TaskScheduler silently overrode the dispatcher's chosen routing,
producing the BDN deadlock + 3.5x perf regression) and centralizes
EC capture/apply in our code (architectural cleanliness; Mechanism
B's leak under SuppressFlow is structurally identical in both
wirings — see test C.2's docstring for context).

Implementation
- SpscAwaiter<T>: 3 new fields (_realContinuation, _realState,
  _capturedEC) + 2 worker-thread scratch fields (_runCb, _runState) +
  IContinuationDispatcher constructor parameter (_dispatcher field).
  Removed _dispatchResult / _dispatchException. OnCompleted captures
  EC on consumer thread, writes fields via Volatile.Write before
  forwarding to _core.OnCompleted with both FlowExecutionContext and
  UseSchedulingContext flags stripped.
- s_dispatch + s_invokeWithEc + s_runContinuation static delegates
  on SpscAwaiter<T>: s_dispatch (registered with _core.OnCompleted)
  routes the awaiter (as state) through _dispatcher; s_invokeWithEc
  reads/clears fields and applies _capturedEC via ExecutionContext.Run
  if non-null. Allocation-free per spec §5 (worker-thread-only scratch
  fields, no per-dispatch boxing).
- SpscPipe.cs / SpscPipe.Reader.cs / SpscPipe.Writer.cs: 12 signal
  sites simplified from stash-and-dispatch (set _dispatch{Result,
  Exception}; DispatchVia(s_dispatchXxx, awaiter)) to direct
  _core.SetResult / _core.SetException calls. The four old
  s_dispatch* delegates and the DispatchVia helper are deleted.
- IContinuationDispatcher implementations require ZERO source changes.
  HotHandoffContinuationDispatcher and ThreadPoolContinuationDispatcher
  remain as-is; the dispatcher is now genuinely a thread router that
  takes (Action<object?>, object?) work items.

Tests
- New: MultiCycle_PerCycleEcCapture_AppliesCorrectEcEachCycle (forward
  regression guard for s_invokeWithEc's per-cycle field reset).
- Updated: CustomDispatcher_BadImpl_CapturingEC_IsDetectable docstring
  to reflect new mechanism (source-side capture beats the bad
  dispatcher to the punch); same observable assertions.
- Existing tests pass unchanged (the SpscPipe-level EC tests, the
  HotHandoff dispatcher tests including the BDN-pattern stress
  RepeatedIteration_PerMessageConsumer_DoesNotHang, the cancellation/
  lifecycle/AdvanceTo tests, the BCL parity tests, and the SpscAwaiter
  unit tests with constructor parameter adjusted).

Spec reference: docs/superpowers/specs/2026-04-28-spsc-awaiter-source-side-ec-capture-design.md

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Test C.3 — `FlowExecutionContext` suppression branch (smoke)

Pins that the `_capturedEC == null` else-branch in `s_invokeWithEc` is exercised cleanly: when the consumer awaits inside an `ExecutionContext.SuppressFlow()` block, the awaiter captures `null` EC, `s_invokeWithEc` falls through to direct `cont(st)` invocation (no `ExecutionContext.Run`), the buffer is delivered, the continuation runs without NRE.

**Limitation acknowledged.** The spec §6 C.3 entry describes a stronger property — "no AsyncLocal pollution from prior cb leaks in" — but that property is not differentially testable between the old and new wirings: under both wirings, default-flow prior cycles run inside an EC frame (`MRVTSC.RunInternal` in old; our `ExecutionContext.Run` in new) which restores the worker's pre-call EC, so the worker thread's EC carries no drift from default-flow prior cbs in either wiring. SuppressFlow on *both* a prior and the current cycle does drift the worker, identically in both wirings. So C.3's testable property reduces to "the SuppressFlow branch in `s_invokeWithEc` exists, is exercised, and doesn't NRE on null EC" — a smoke test, not a regression for a fixed bug.

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs`

- [ ] **Step 1: Write the test**

Append this test method to the `SpscPipeContinuationDispatcherTests` class:

```csharp
// ---------- C.3 — FlowExecutionContext suppressed (smoke for null-_capturedEC branch) ----------

/// <summary>
/// When the consumer awaits inside an ExecutionContext.SuppressFlow() block,
/// SpscAwaiter.OnCompleted captures _capturedEC = null and forwards (s_dispatch,
/// this) to _core.OnCompleted. s_invokeWithEc reads _capturedEC, sees null, and
/// takes the else branch — direct cont(st) invocation on the dispatcher's chosen
/// thread, no ExecutionContext.Run. This test pins that the branch is exercised
/// cleanly (no NRE on null EC, buffer delivered, await completes).
///
/// Note: the spec §6 C.3 entry describes a stronger "no leak from prior cb"
/// property, but that property is structurally identical in old and new wirings
/// (both let SuppressFlow cbs mutate the worker's EC, both isolate default-flow
/// cbs in an EC frame). The differentiating test would require both wirings to
/// behave differently under the same input, which they don't for SuppressFlow
/// AsyncLocal observation. See the C.2 docstring for the same caveat applied
/// to per-cycle isolation.
/// </summary>
[Fact]
public async Task SuppressFlow_AtAwait_NoCapturedEC_BranchExercisedCleanly()
{
    using var dispatcher = new ForwardingDispatcher();
    using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

    _ = Task.Run(async () =>
    {
        await Task.Delay(50);
        var mem = pipe.Writer.GetMemory(5);
        mem.Span.Clear();
        pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();
    });

    using (ExecutionContext.SuppressFlow())
    {
        // Inside SuppressFlow, the await's OnCompleted is called with
        // FlowExecutionContext = 0, so SpscAwaiter.OnCompleted captures
        // _capturedEC = null. s_invokeWithEc takes the null-EC branch and
        // invokes the continuation directly. No EC capture, no Run, no
        // restoration — the simplest path. The assertion pins the buffer
        // is delivered (no NRE / no hang).
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);
        Assert.Equal(5, rr.Buffer.Length);
    }
}
```

- [ ] **Step 2: Run the test**

Run:

```bash
dotnet test tests/SpscPipe.Tests --nologo --filter SuppressFlow_AtAwait_NoCapturedEC_BranchExercisedCleanly
```

Expected: PASS. The test only asserts the buffer length and that the await completes; the spec invariant being pinned is "the null-EC code path runs without NRE."

- [ ] **Step 3: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs
git commit -m "$(cat <<'EOF'
SpscAwaiter tests: C.3 — FlowExecutionContext suppression branch

Pins the spec invariant that when the consumer awaits inside
ExecutionContext.SuppressFlow(), s_invokeWithEc takes the null-EC
branch and invokes the continuation directly (no ExecutionContext.Run).
Verifies the branch is exercised cleanly (no NRE, no hang, buffer
delivered).

Spec ref: §2.3 (s_invokeWithEc null-_capturedEC branch), §6 C.3.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Test D.1 — `SynchronizationContext` capture not honored

Pins the public-contract clause from spec §3.3: a non-default `SynchronizationContext` set at the `await` site is bypassed. The continuation runs on the dispatcher's chosen thread, not the SC's thread. The implementation pins this by stripping `UseSchedulingContext` from the flags forwarded to `_core.OnCompleted` — so MRVTSC never captures the SC.

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs`

- [ ] **Step 1: Write the failing test (and the helper SC type)**

Append to `SpscPipeContinuationDispatcherTests` — the helper class first (above the existing `// ---------- Tests ----------` header is a natural spot, but appending at the end is also fine; keep alphabetical / topical proximity to the other helper dispatchers if convenient):

```csharp
// ---------- D.1 helper — capturing SynchronizationContext ----------

/// <summary>
/// SynchronizationContext that records every Post call. If the consumer's await captured
/// this SC and posted the continuation through it, PostCount > 0. The test asserts
/// PostCount == 0 — the new wiring strips UseSchedulingContext, so MRVTSC never captures
/// the SC.
/// </summary>
private sealed class CapturingSynchronizationContext : SynchronizationContext
{
    public int PostCount;
    public override void Post(SendOrPostCallback d, object? state)
    {
        Interlocked.Increment(ref PostCount);
        ThreadPool.UnsafeQueueUserWorkItem(_ => d(state), null);
    }
}
```

Then the test method:

```csharp
// ---------- D.1 — SynchronizationContext at await site is NOT honored ----------

/// <summary>
/// A non-default SynchronizationContext set at the await site is NOT honored: the
/// continuation runs on the dispatcher's chosen thread, NOT on the SC's thread. The
/// new SpscAwaiter.OnCompleted strips UseSchedulingContext from the flags forwarded
/// to _core.OnCompleted, so MRVTSC does not capture the SC. The captured SC's
/// PostCount stays 0; the continuation thread name is the dispatcher's thread.
/// </summary>
[Fact]
public async Task SynchronizationContext_AtAwait_NotHonored_ContinuationOnDispatcherThread()
{
    using var dispatcher = new DedicatedThreadDispatcher();
    using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

    var sc = new CapturingSynchronizationContext();
    var prev = SynchronizationContext.Current;
    SynchronizationContext.SetSynchronizationContext(sc);

    string? observedThreadName = null;
    int? observedThreadId = null;
    try
    {
        _ = Task.Run(async () =>
        {
            await Task.Delay(50);
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);
        observedThreadName = Thread.CurrentThread.Name;
        observedThreadId   = Environment.CurrentManagedThreadId;
    }
    finally
    {
        SynchronizationContext.SetSynchronizationContext(prev);
    }

    // Captured SC was bypassed — PostCount stays 0.
    Assert.Equal(0, Volatile.Read(ref sc.PostCount));
    // Continuation ran on the dispatcher's worker thread, not the test/SC thread.
    Assert.Equal(nameof(DedicatedThreadDispatcher), observedThreadName);
    Assert.NotEqual(Environment.CurrentManagedThreadId, observedThreadId);
}
```

- [ ] **Step 2: Run the test**

Run:

```bash
dotnet test tests/SpscPipe.Tests --nologo --filter SynchronizationContext_AtAwait_NotHonored_ContinuationOnDispatcherThread
```

Expected: PASS. The new wiring strips `UseSchedulingContext`, so MRVTSC does not capture the SC. The continuation runs on the dispatcher's chosen thread (the `DedicatedThreadDispatcher`'s worker, named `"DedicatedThreadDispatcher"`). The captured SC's `PostCount` stays 0 because the SC's `Post` was never called.

- [ ] **Step 3: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs
git commit -m "$(cat <<'EOF'
SpscAwaiter tests: D.1 — SynchronizationContext capture bypassed

Pins the public-contract clause that a non-default SC set at the
await site is NOT honored: the continuation runs on the dispatcher's
chosen thread, not the SC's thread. The new OnCompleted strips
UseSchedulingContext from the flags forwarded to _core.OnCompleted,
so MRVTSC does not capture the SC. PostCount stays 0; continuation
thread is the dispatcher's worker.

Spec ref: §3.3 SpscPipe public-contract scheduler-bypass clause.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Test D.2 — `TaskScheduler` capture not honored

Pins the second half of spec §3.3: a non-default `TaskScheduler` (e.g., one obtained via `TaskScheduler.FromCurrentSynchronizationContext()`) set on the awaiting task is also bypassed. Same mechanism as D.1 — `UseSchedulingContext` stripping in `OnCompleted` covers both `SynchronizationContext` and `TaskScheduler` (MRVTSC captures both via the same flag).

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs`

- [ ] **Step 1: Write the failing test**

Append to `SpscPipeContinuationDispatcherTests`:

```csharp
// ---------- D.2 — TaskScheduler at await site is NOT honored ----------

/// <summary>
/// A non-default TaskScheduler captured by the consumer's await (here, via
/// TaskScheduler.FromCurrentSynchronizationContext on a custom SC) is NOT honored.
/// Same mechanism as D.1: stripping UseSchedulingContext in OnCompleted prevents
/// MRVTSC from capturing the scheduler. The continuation runs on the dispatcher's
/// chosen thread, not the scheduler's thread.
/// </summary>
[Fact]
public async Task TaskScheduler_AtAwait_NotHonored_ContinuationOnDispatcherThread()
{
    using var dispatcher = new DedicatedThreadDispatcher();
    using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

    // Bind a custom SC; from it derive a TaskScheduler (the standard idiom for
    // single-threaded UI-style scheduling). Run the test body via Task.Factory.StartNew
    // with that scheduler so the await's captured TaskScheduler is the custom one.
    var sc = new CapturingSynchronizationContext();
    var prev = SynchronizationContext.Current;
    SynchronizationContext.SetSynchronizationContext(sc);
    string? observedThreadName = null;
    int? observedThreadId = null;
    try
    {
        var scheduler = TaskScheduler.FromCurrentSynchronizationContext();

        await Task.Factory.StartNew(async () =>
        {
            _ = Task.Run(async () =>
            {
                await Task.Delay(50);
                var mem = pipe.Writer.GetMemory(5);
                mem.Span.Clear();
                pipe.Writer.Advance(5);
                await pipe.Writer.FlushAsync();
            });

            var rr = await pipe.Reader.ReadAsync();
            pipe.Reader.AdvanceTo(rr.Buffer.End);
            observedThreadName = Thread.CurrentThread.Name;
            observedThreadId   = Environment.CurrentManagedThreadId;
        }, default, TaskCreationOptions.None, scheduler).Unwrap();
    }
    finally
    {
        SynchronizationContext.SetSynchronizationContext(prev);
    }

    // Captured scheduler was bypassed — the SC's PostCount stays 0 (TaskScheduler.FromCurrentSynchronizationContext
    // routes through Post, so it shares the PostCount with D.1).
    Assert.Equal(0, Volatile.Read(ref sc.PostCount));
    Assert.Equal(nameof(DedicatedThreadDispatcher), observedThreadName);
    Assert.NotEqual(Environment.CurrentManagedThreadId, observedThreadId);
}
```

- [ ] **Step 2: Run the test**

Run:

```bash
dotnet test tests/SpscPipe.Tests --nologo --filter TaskScheduler_AtAwait_NotHonored_ContinuationOnDispatcherThread
```

Expected: PASS. The continuation runs on the dispatcher's worker thread, not the scheduler's thread.

- [ ] **Step 3: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs
git commit -m "$(cat <<'EOF'
SpscAwaiter tests: D.2 — TaskScheduler capture bypassed

Pins the second half of the public-contract scheduler-bypass clause:
a non-default TaskScheduler captured by the consumer's await is NOT
honored. Same mechanism as D.1 (UseSchedulingContext flag stripping
in OnCompleted covers both SC and TaskScheduler).

Spec ref: §3.3 SpscPipe public-contract scheduler-bypass clause.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Test D.3 — `ConfigureAwait(true)` and `ConfigureAwait(false)` parity

Pins the spec §6 D.3 invariant: with the new wiring, `ConfigureAwait(true)` and `ConfigureAwait(false)` produce identical observable behavior — both run the continuation on the dispatcher's chosen thread. (Under the OLD wiring, `ConfigureAwait(true)` was the load-bearing source of Mechanism A: it told the await to capture the current SC/TaskScheduler, which then posted the continuation back to that scheduler regardless of the dispatcher's choice.)

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs`

- [ ] **Step 1: Write the failing test**

Append to `SpscPipeContinuationDispatcherTests`:

```csharp
// ---------- D.3 — ConfigureAwait(true) vs ConfigureAwait(false) parity ----------

/// <summary>
/// With the new source-side EC-capture wiring, ConfigureAwait(true) and
/// ConfigureAwait(false) produce identical observable behavior on a SpscPipe await:
/// both run the continuation on the dispatcher's chosen thread regardless of the
/// consumer's captured SC/TaskScheduler. This was the original Mechanism A pin —
/// the BDN deadlock disappeared when ConfigureAwait(false) was added; with the
/// new wiring, both directions are equivalent because the SC is never captured
/// (UseSchedulingContext is stripped in OnCompleted).
/// </summary>
[Theory]
[InlineData(true)]
[InlineData(false)]
public async Task ConfigureAwait_TrueAndFalse_BothRunOnDispatcherThread(bool continueOnCapturedContext)
{
    using var dispatcher = new DedicatedThreadDispatcher();
    using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

    var sc = new CapturingSynchronizationContext();
    var prev = SynchronizationContext.Current;
    SynchronizationContext.SetSynchronizationContext(sc);

    string? observedThreadName = null;
    int? observedThreadId = null;
    try
    {
        _ = Task.Run(async () =>
        {
            await Task.Delay(50);
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        var rr = await pipe.Reader.ReadAsync().ConfigureAwait(continueOnCapturedContext);
        pipe.Reader.AdvanceTo(rr.Buffer.End);
        observedThreadName = Thread.CurrentThread.Name;
        observedThreadId   = Environment.CurrentManagedThreadId;
    }
    finally
    {
        SynchronizationContext.SetSynchronizationContext(prev);
    }

    // Both ConfigureAwait(true) and ConfigureAwait(false) yield identical results:
    // the SC is never captured (PostCount stays 0), and the continuation runs on
    // the dispatcher's worker thread.
    Assert.Equal(0, Volatile.Read(ref sc.PostCount));
    Assert.Equal(nameof(DedicatedThreadDispatcher), observedThreadName);
    Assert.NotEqual(Environment.CurrentManagedThreadId, observedThreadId);
}
```

- [ ] **Step 2: Run the test**

Run:

```bash
dotnet test tests/SpscPipe.Tests --nologo --filter ConfigureAwait_TrueAndFalse_BothRunOnDispatcherThread
```

Expected: PASS for both `true` and `false` parameter values.

- [ ] **Step 3: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs
git commit -m "$(cat <<'EOF'
SpscAwaiter tests: D.3 — ConfigureAwait(true/false) parity

Pins the spec invariant that with the new source-side EC-capture
wiring, ConfigureAwait(true) and ConfigureAwait(false) produce
identical observable behavior on a SpscPipe await: both run the
continuation on the dispatcher's chosen thread. The SC is never
captured because OnCompleted strips UseSchedulingContext, so the
consumer's continueOnCapturedContext choice is irrelevant.

This was the original Mechanism A pin — adding ConfigureAwait(false)
in BDN's harness made the deadlock disappear; with the new wiring,
both directions are equivalent.

Spec ref: §6 D.3, §3.3 scheduler-bypass clause.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Test E.1 — `SetResult`-fires-first race stress (direct-awaiter)

Pins the spec §4 publication ordering: when the producer signals BEFORE the consumer has called `OnCompleted`, the result is delivered correctly via the rare TP-dispatch path. `MRVTSC` unconditionally queues the registered `s_dispatch` callback to the ThreadPool when the source is already completed; the `Volatile.Write` ordering in `OnCompleted` ensures the TP-dispatched `s_dispatch` reads `_realContinuation` / `_realState` / `_capturedEC` post-publication.

This test operates **directly on a `SpscAwaiter<int>`** rather than going through `SpscPipe`. `SpscPipe.Reader.ReadAsync` has a synchronous fast path (`HasReadableProgress` after `TryAcquire`+integrate) that delivers data without ever calling `OnCompleted` if the producer has already published — so a SpscPipe-level test of "producer-flushes-before-consumer-awaits" never exercises the rare race path. Operating directly on `SpscAwaiter` lets us call `_core.SetResult(value)` first and then manually invoke `awaiter.OnCompleted(...)`, which forces `MRVTSC` to see a completed source and queue `s_dispatch` to TP — the actual race the spec calls out. `SpscAwaiter<T>` is `internal` and `SpscPipe.Tests` already has `InternalsVisibleTo` access.

**Files:**
- Modify: `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs`

- [ ] **Step 1: Write the test**

Append to `SpscPipeContinuationDispatcherTests`:

```csharp
// ---------- E.1 — SetResult-fires-first race (direct-awaiter, N-iteration stress) ----------

/// <summary>
/// Pins the spec §4 publication ordering: when the producer signals BEFORE the
/// consumer has called OnCompleted, the result is delivered correctly via the rare
/// TP-dispatch path. MRVTSC unconditionally queues the registered s_dispatch
/// callback to the ThreadPool when the source is already completed at OnCompleted
/// time; the Volatile.Write ordering in OnCompleted ensures the TP-dispatched
/// s_dispatch reads _realContinuation / _realState / _capturedEC post-publication.
///
/// Operates directly on SpscAwaiter to force the race deterministically (SpscPipe's
/// synchronous fast paths would short-circuit before OnCompleted is even called).
/// Stress N iterations to expose any non-deterministic ordering bug under
/// CI/jit/scheduler variance.
/// </summary>
[Fact]
public async Task SetResultBeforeOnCompleted_DirectAwaiter_Race_StressN_AllResultsDelivered()
{
    using var dispatcher = new ForwardingDispatcher();

    const int iterations = 100;
    for (int i = 0; i < iterations; i++)
    {
        // Construct a fresh awaiter per iteration. The dispatcher is shared across
        // iterations (ForwardingDispatcher just routes to TP).
        var awaiter = new SpscAwaiter<int>(dispatcher);

        // PRODUCER SIGNALS FIRST. _core stores the result; _core's _continuation
        // is null because OnCompleted hasn't been called yet.
        awaiter._core.SetResult(1000 + i);

        // CONSUMER REGISTERS SECOND (manually). SpscAwaiter.OnCompleted writes
        // _realContinuation / _realState / _capturedEC via Volatile.Write, then
        // forwards (s_dispatch, this, ...) to _core.OnCompleted. _core sees a
        // completed source and unconditionally queues s_dispatch to TP. TP runs
        // s_dispatch, which routes through dispatcher to s_invokeWithEc, which
        // reads the awaiter's published fields and invokes our continuation under
        // the captured EC.
        var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        awaiter.OnCompleted(_ =>
        {
            try { tcs.SetResult(awaiter._core.GetResult(awaiter.Version)); }
            catch (Exception ex) { tcs.SetException(ex); }
        }, state: null, awaiter.Version, ValueTaskSourceOnCompletedFlags.FlowExecutionContext);

        int result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1000 + i, result);
    }
}
```

Note: this test requires `using System.Threading.Tasks.Sources;` at the top of the file for `ValueTaskSourceOnCompletedFlags`. If that using is not already present, add it. Verify with:

```bash
grep -n 'using System.Threading.Tasks.Sources' tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs
```

If missing, add the using directive at the top of the file alongside the existing `using System.Threading;`.

- [ ] **Step 2: Run the test**

Run:

```bash
dotnet test tests/SpscPipe.Tests --nologo --filter SetResultBeforeOnCompleted_DirectAwaiter_Race_StressN_AllResultsDelivered
```

Expected: PASS. 100 iterations; every iteration's `tcs.Task` completes within the 5s WaitAsync timeout with `result == 1000 + i`. No `IValueTaskSource`-version-mismatch exceptions, no hangs, no `OperationCanceledException` from the timeout. Each iteration drives the rare race (because `_core.SetResult` was called before `awaiter.OnCompleted`), so the stress meaningfully exercises `MRVTSC`'s "queue to TP if source already completed" branch and our publication-ordering correctness.

- [ ] **Step 3: Commit**

```bash
git add tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs
git commit -m "$(cat <<'EOF'
SpscAwaiter tests: E.1 — SetResult-fires-first race (direct-awaiter stress)

Pins the spec §4 publication ordering by operating directly on
SpscAwaiter<int> rather than through SpscPipe (whose synchronous
fast paths would bypass the race). Each iteration calls
_core.SetResult(value) BEFORE manually invoking awaiter.OnCompleted,
forcing MRVTSC to see a completed source and unconditionally queue
s_dispatch to the ThreadPool. The TP-dispatched s_dispatch routes
through the dispatcher to s_invokeWithEc, which reads the awaiter's
published _realContinuation/_realState/_capturedEC and invokes the
continuation under the captured EC.

100 iterations expose CI/jit/scheduler variance against ordering
bugs. Volatile.Write ordering in OnCompleted closes the race
across the queue/dequeue happens-before edges.

Spec ref: §4 publication ordering, §6 E.1.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Update `docs/IContinuationDispatcher.md` — contract item #2 + EC contract section + scheduler-bypass clause

Per spec §3.1 and §3.2, the public-facing contract document needs three updates:
1. Contract item #2's text changes from "MUST NOT capture EC" to a revised description that says EC handling is now performed by `SpscAwaiter<T>` (source-side capture/apply); the dispatcher is purely a thread router.
2. The "EC contract: why it matters" section needs to be rewritten to describe the new flow — source-side capture at `OnCompleted`, dispatcher-side application via `ExecutionContext.Run` in `s_invokeWithEc`, `MRVTSC.RunInternal` no longer in the picture.
3. A new clause explaining the scheduler bypass (`SynchronizationContext` / `TaskScheduler` not honored) — quoting spec §3.3.
4. The "Where to look in the code" table needs the stash-fields row revised (now `_realContinuation` / `_realState` / `_capturedEC`) and the static-dispatch-delegates row revised (now `s_dispatch` / `s_invokeWithEc` on `SpscAwaiter.cs`).

**Files:**
- Modify: `docs/IContinuationDispatcher.md`

- [ ] **Step 1: Read the file to confirm the line ranges to edit**

Run:

```bash
grep -n -E '^## |^### |MUST NOT capture' docs/IContinuationDispatcher.md
```

Expected output identifies the section structure (lines vary slightly):
- `## Contract (must be followed by all implementations)` at line ~36
- `## EC contract: why it matters` at line ~134
- `## Where to look in the code` at line ~149

- [ ] **Step 2: Replace contract item #2**

Find this text in `docs/IContinuationDispatcher.md`:

```
2. **The implementation MUST NOT capture or apply an `ExecutionContext`.** SpscPipe relies on `ManualResetValueTaskSourceCore`'s internal EC restoration (using the consumer-captured EC from `OnCompleted` time) to scope the continuation correctly. Adding EC manipulation in the dispatcher will leak the *producer's* EC into the continuation in the rare case where the consumer's `await` suppressed `FlowExecutionContext`. For TP-based dispatchers, use `ThreadPool.UnsafeQueueUserWorkItem` (NOT the safe `QueueUserWorkItem` or `Task.Run`, both of which capture EC implicitly). For dedicated-thread dispatchers, hand off the callback delegate as-is.
```

Replace with:

```
2. **The implementation MUST NOT capture or apply an `ExecutionContext`.** EC handling for `SpscPipe`'s awaitable continuations is performed by `SpscAwaiter<T>`: it captures the consumer's `ExecutionContext` at `OnCompleted` time (per the consumer's `FlowExecutionContext` flag), passes the dispatcher a work item that carries the captured EC alongside the continuation (via fields on the awaiter — the awaiter is the `state` argument), and applies the EC via `ExecutionContext.Run` at invoke time. The dispatcher is purely a thread router. Adding EC manipulation in the dispatcher would interfere with the source-side capture/apply protocol and is forbidden. For TP-based dispatchers, use `ThreadPool.UnsafeQueueUserWorkItem` (NOT the safe `QueueUserWorkItem` or `Task.Run`, both of which capture EC implicitly). For dedicated-thread dispatchers, hand off the callback delegate as-is.
```

- [ ] **Step 3: Replace the "EC contract: why it matters" section in full**

Find the entire section starting `## EC contract: why it matters` through the end of the section (just before `## Where to look in the code`). The section currently begins:

```
## EC contract: why it matters

When the consumer's `await pipe.Reader.ReadAsync()` suspends, the runtime captures the consumer's current `ExecutionContext` (which carries `AsyncLocal<T>` values, ambient diagnostic state, etc.) **on the consumer's thread, at `OnCompleted` time**. That captured EC is stored on the awaiter's `_core` field.
```

Replace the entire `## EC contract: why it matters` section with:

```
## EC contract: how it works (source-side capture)

When the consumer's `await pipe.Reader.ReadAsync()` suspends, **`SpscAwaiter<T>.OnCompleted`** captures the consumer's current `ExecutionContext` (which carries `AsyncLocal<T>` values, ambient diagnostic state, etc.) on the consumer's thread, at the moment of the `await`. The capture is gated by the `ValueTaskSourceOnCompletedFlags.FlowExecutionContext` flag the consumer's await machinery passed in: if the flag is set (the default), `_capturedEC = ExecutionContext.Capture()`; if the flag is cleared (the consumer is inside an `ExecutionContext.SuppressFlow()` block), `_capturedEC = null` — the consumer has explicitly opted out of EC propagation.

`SpscAwaiter<T>` then writes the consumer-supplied `(continuation, state)` pair to its `_realContinuation` / `_realState` fields and forwards a different `(callback, state)` pair to `_core.OnCompleted` — specifically, its own internal `s_dispatch` delegate paired with `this` (the awaiter). It also strips both `FlowExecutionContext` and `UseSchedulingContext` from the flags forwarded to `_core.OnCompleted`:

- **`FlowExecutionContext` is stripped** because `SpscAwaiter` already captured the EC; leaving the flag on would have `MRVTSC` capture again (redundant).
- **`UseSchedulingContext` is stripped** because `SpscPipe`'s public contract is that the configured `IContinuationDispatcher` controls continuation routing — the consumer's captured `SynchronizationContext` / `TaskScheduler` is intentionally ignored.

When the producer signals (`_core.SetResult` / `_core.SetException`), `MRVTSC` invokes the registered `s_dispatch` callback inline on the producer's thread (because `RunContinuationsAsynchronously = false`). `s_dispatch` calls `dispatcher.UnsafeQueueUserWorkItem(s_invokeWithEc, awaiter)` — handing the work item to the configured dispatcher. The dispatcher's chosen thread invokes `s_invokeWithEc`, which:

1. Reads and clears `_realContinuation`, `_realState`, `_capturedEC`.
2. If `_capturedEC` is non-null, calls `ExecutionContext.Run(_capturedEC, s_runContinuation, awaiter)` — applying the consumer's captured EC for the duration of the continuation invocation; the dispatcher thread's pre-call EC is automatically saved and restored by `ExecutionContext.Run`.
3. If `_capturedEC` is null (consumer suppressed flow), invokes the continuation directly on the dispatcher's chosen thread — the consumer explicitly opted out of EC propagation and accepts whatever EC that thread has.

The EC isolation guarantee `SpscPipe` gives the consumer is therefore: **regardless of the `IContinuationDispatcher` configured, your `await pipe.Reader.ReadAsync()` continuation runs under the `ExecutionContext` your code had at the `await` — same as standard `Task.Run` / `await` semantics — provided `FlowExecutionContext` was set at `OnCompleted` (the default). This guarantee is robust against worker-thread-EC drift in dispatchers with long-lived worker threads (e.g., `HotHandoffContinuationDispatcher`).**

A dispatcher that captures EC itself (e.g., uses the EC-capturing `ThreadPool.QueueUserWorkItem` instead of the recommended `UnsafeQueueUserWorkItem`) does NOT break the consumer's EC guarantee — `s_invokeWithEc` applies the consumer-captured EC after the dispatcher's hop — but it DOES introduce wasteful capture/apply overhead and violates contract item #2.

## Scheduler bypass

`SpscPipe`'s `Reader.ReadAsync` and `Writer.FlushAsync` continuations do **not** honor the consumer's captured `SynchronizationContext` or `TaskScheduler`. The continuation runs on the thread chosen by the configured `IContinuationDispatcher` (default: the .NET `ThreadPool` via `ThreadPoolContinuationDispatcher`). This is independent of the consumer's `ConfigureAwait(true|false)` choice — both produce identical observable behavior. Consumers requiring continuation on a specific scheduler should either:

- (a) post explicitly via `SynchronizationContext.Post` / `TaskScheduler.FromCurrentSynchronizationContext().StartNew` after the `await`, or
- (b) wrap the awaitable in a `Task.Run` to capture context boundaries.

This is a deliberate contract choice, not an implementation accident. `SpscPipe` is a high-throughput primitive aimed at server-side workloads where consumer-side scheduler capture is not the desired routing. The explicit contract clause prevents surprise.
```

- [ ] **Step 4: Replace the "Where to look in the code" table**

Find the existing table after `## Where to look in the code`. It currently contains the `_dispatchResult` / `_dispatchException` row and the `s_dispatch*` / `DispatchVia` row, both of which point to obsolete code. Replace the entire table with:

```markdown
| What | Where |
|------|-------|
| Interface + default impl | `src/SpscPipelines/IContinuationDispatcher.cs` |
| Options field | `src/SpscPipelines/SpscPipeOptions.cs` |
| Source-side EC capture (consumer-thread) | `src/SpscPipelines/SpscAwaiter.cs` (`OnCompleted` override; `_realContinuation` / `_realState` / `_capturedEC` fields) |
| Source-side EC application (dispatcher-thread) | `src/SpscPipelines/SpscAwaiter.cs` (`s_dispatch`, `s_invokeWithEc`, `s_runContinuation` static delegates) |
| Signal-path SetResult/SetException sites | All in `SpscPipe.cs`, `SpscPipe.Reader.cs`, `SpscPipe.Writer.cs` — direct `_core.SetResult` / `_core.SetException` calls; the dispatcher hop is encapsulated inside `SpscAwaiter`'s `OnCompleted` + `s_dispatch` flow |
| Tests (EC flow, isolation, scheduler bypass, race) | `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs` |
```

- [ ] **Step 5: Verify the changes don't break any cross-references**

Run:

```bash
grep -n '_dispatchResult\|_dispatchException\|s_dispatchReadSetResult\|s_dispatchReadSetException\|s_dispatchFlushSetResult\|s_dispatchFlushSetException\|DispatchVia' docs/IContinuationDispatcher.md
```

Expected: no matches.

- [ ] **Step 6: Commit**

```bash
git add docs/IContinuationDispatcher.md
git commit -m "$(cat <<'EOF'
docs: revise IContinuationDispatcher contract for source-side EC capture

Three updates per the new spec:
- Contract item #2: text now states EC handling is performed by
  SpscAwaiter<T> (source-side capture/apply); dispatcher is purely a
  thread router. The MUST-NOT-capture rule still holds.
- "EC contract" section rewritten in full to describe the new flow:
  capture at consumer-thread OnCompleted, apply on dispatcher-thread
  via ExecutionContext.Run in s_invokeWithEc. Removes the obsolete
  description of MRVTSC.RunInternal handling EC.
- New "Scheduler bypass" section documenting that SC/TaskScheduler
  set at the await site are NOT honored — continuation runs on the
  dispatcher's chosen thread regardless of ConfigureAwait choice.
- "Where to look in the code" table updated to point to the new
  field/delegate names (_realContinuation, _realState, _capturedEC,
  s_dispatch, s_invokeWithEc) and to drop the obsolete _dispatchResult
  / _dispatchException / s_dispatch* / DispatchVia references.

Spec ref: §3.1, §3.2, §3.3.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Update SpscPipe spec — §5 fields, §5 continuation dispatch, §6 scheduler-bypass clause, I16

Per spec §8 Spec references: the `SpscPipe` design spec (`docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md`) needs three updates:
1. §5 awaiter shape — replace `_dispatchResult` / `_dispatchException` field references with the new `_realContinuation` / `_realState` / `_capturedEC` triple. Update the "Field-access discipline" subsection's bullet about dispatch-stash fields.
2. §5 "Continuation dispatch" — rewrite the protocol description from stash-and-dispatch-shorthand (the "shorthand for stash + dispatch" paragraph) to "the signal site calls `_core.SetResult/SetException` directly; `s_dispatch` (registered with `_core` via `OnCompleted`) routes to the dispatcher inline."
3. §6 (`IContinuationDispatcher` public contract) — append the scheduler-bypass clause from spec §3.3 verbatim, and update I16 (EC capture / restoration discipline invariant) to describe the new source-side flow.

**Files:**
- Modify: `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md`

- [ ] **Step 1: Replace the awaiter field declaration in §5**

Find this block (around line 715-722):

```csharp
    // Stash for the dispatcher hop. Set by the actor that wins CAS Pending→Inactive (signaler /
    // canceler / token-callback / park re-checks — both lost-wakeup and lost-cancel) before
    // invoking the dispatcher; read by the dispatched callback, which clears the field and then
    // calls _core.SetResult / _core.SetException. See "Continuation dispatch" below.
    public T?         _dispatchResult;
    public Exception? _dispatchException;
```

Replace with:

```csharp
    // Source-side EC-capture stash. Set by SpscAwaiter.OnCompleted on the consumer's thread
    // BEFORE delegating to _core.OnCompleted (visible by the time s_dispatch reads them, in
    // both the OnCompleted-fires-first and SetResult-fires-first races; see §4 publication
    // ordering in the source-side EC-capture spec). Read by s_invokeWithEc on the dispatcher's
    // chosen thread; cleared after read.
    public Action<object?>? _realContinuation;
    public object?          _realState;
    public ExecutionContext? _capturedEC;
```

- [ ] **Step 2: Update the field-access discipline bullet about the dispatch stash**

Find this bullet (around line 743):

```
- Dispatch-stash fields (`_dispatchResult`, `_dispatchException`) are written by whichever actor wins CAS Pending→Inactive (signaler / canceler / token callback / park re-checks — both lost-wakeup and lost-cancel), *after* the CAS but *before* the call to `IContinuationDispatcher.UnsafeQueueUserWorkItem`. Read inside the dispatched callback, which clears the field (sets it back to `default` / `null`) and then invokes `_core.SetResult` / `_core.SetException`. Because exactly one actor wins the CAS per park cycle (I11), there is no concurrent writer; the read inside the callback synchronizes with that single writer through the dispatcher's own ordering guarantees (any sound implementation of `UnsafeQueueUserWorkItem` provides a happens-before edge from the queue-call to the dequeued callback). Clearing on read is hygiene: it releases the awaiter's reference to the just-delivered `ReadResult` (and its `ROS<byte>` of segments) and to any stashed exception, so the awaiter does not retain those references between park cycles.
```

Replace with:

```
- Source-side EC stash fields (`_realContinuation`, `_realState`, `_capturedEC`) are written by `SpscAwaiter.OnCompleted` on the **consumer's thread** before delegating to `_core.OnCompleted`. They are read by `s_invokeWithEc` on the **dispatcher's chosen thread** (after `s_dispatch` queued the work item via `IContinuationDispatcher.UnsafeQueueUserWorkItem`). Visibility chain: `OnCompleted`'s `Volatile.Write` ⟹ `_core.OnCompleted`'s register/queue ⟹ (in the OnCompleted-fires-first race) `_core.SetResult`/`SetException` invoking `s_dispatch` inline ⟹ dispatcher's `UnsafeQueueUserWorkItem` happens-before to dequeued `s_invokeWithEc`; or (in the SetResult-fires-first race) `MRVTSC` queues `s_dispatch` to TP, TP dequeue HB to dispatcher's queue, dispatcher's HB to `s_invokeWithEc`. Either path makes the writes visible to the reads. `s_invokeWithEc` clears the fields after reading (hygiene — releases references to delivered `ReadResult`/exception/EC between park cycles).
```

- [ ] **Step 3: Replace the "Continuation dispatch" subsection's protocol description**

Find this passage (around lines 749-790, starting with "The protocol is:" and ending with "**Allocation cost.**" inclusive of the final paragraph):

```
The protocol is:

1. CAS Pending → Inactive succeeds (the actor has won the right to deliver the result).
2. Stash the result or exception on the awaiter (`_dispatchResult` or `_dispatchException`).
3. Call `dispatcher.UnsafeQueueUserWorkItem(s_dispatchCallback, awaiter)`, where `dispatcher` is `_options.ContinuationDispatcher ?? ThreadPoolContinuationDispatcher.Instance` and `s_dispatchCallback` is one of four static delegates (allocated once at type-init) that:
   - Reads the stashed value back off the awaiter,
   - Clears the stash field,
   - Calls `awaiter._core.SetResult(value)` (or `awaiter._core.SetException(exception)`) on the dispatcher's chosen thread.
4. With `_core.RunContinuationsAsynchronously = false`, that `_core.SetResult` / `_core.SetException` runs the registered continuation inline under the consumer-captured ExecutionContext (see I16: EC capture / restoration discipline, in this section's invariants list).

The four static delegates correspond to `(SetResult|SetException) × (ReadAwaiter|FlushAwaiter)`:

- `s_dispatchReadSetResult`
- `s_dispatchReadSetException`
- `s_dispatchFlushSetResult`
- `s_dispatchFlushSetException`

In the pseudocode below, every `_xxxAwaiter._core.SetResult(value)` and `_xxxAwaiter._core.SetException(ex)` is shorthand for the following stash-and-dispatch sequence (the read/SetResult flavor shown; parallel forms for `SetException` and for the flush awaiter):

```csharp
// shorthand: _readAwaiter._core.SetResult(value)
//   expands to ↓
_readAwaiter._dispatchResult = value;
(_options.ContinuationDispatcher ?? ThreadPoolContinuationDispatcher.Instance)
    .UnsafeQueueUserWorkItem(s_dispatchReadSetResult, _readAwaiter);

// where:
private static readonly Action<object?> s_dispatchReadSetResult = static state => {
    var a = (SpscAwaiter<ReadResult>)state!;
    var v = a._dispatchResult;
    a._dispatchResult = default;
    a._core.SetResult(v);
};
```

Lost-wakeup re-check paths inside `ParkReadAwaiter` / `ParkFlushAwaiter` that **don't** go through `_core.Set*` (the data-return paths that build a `ValueTask<T>` directly via `BuildReadResult` / `BuildFlushResult`) are *not* dispatched — there is no continuation, the result is returned synchronously to the caller's await machinery.

**Allocation cost.** Zero allocations per dispatch: the static delegates are allocated once at type-init; the result/exception is stashed on the existing awaiter object (no per-dispatch tuple/closure); the dispatcher itself receives `(Action<object?>, object?)` matching `ThreadPool.UnsafeQueueUserWorkItem`'s shape. Per-dispatch overhead vs. RCA = true: one extra virtual call into the dispatcher (~1-2 ns); should be invisible in throughput benchmarks.
```

Replace with:

```
The protocol is:

1. **Consumer's `await` reaches `OnCompleted`.** `SpscAwaiter.OnCompleted` runs on the consumer's thread, captures `ExecutionContext` (gated by `FlowExecutionContext`), writes `_realContinuation` / `_realState` / `_capturedEC` via `Volatile.Write`, and forwards `(s_dispatch, this, token, flags & ~suppressed)` to `_core.OnCompleted` — where `suppressed = FlowExecutionContext | UseSchedulingContext`.
2. **Producer signals.** Some actor (signaler / canceler / token callback / park re-check) wins CAS Pending→Inactive and calls `_xxxAwaiter._core.SetResult(value)` or `_core.SetException(ex)` directly on the producer's thread (no stashing).
3. **MRVTSC dispatches.** `_core.RunContinuationsAsynchronously = false`, so `_core.SetResult` / `_core.SetException` invokes the registered `s_dispatch` callback inline on the producer's thread.
4. **`s_dispatch` routes through the dispatcher.** It reads `awaiter._dispatcher` (set at construction) and calls `dispatcher.UnsafeQueueUserWorkItem(s_invokeWithEc, awaiter)`. The dispatcher hands the work item to its chosen thread.
5. **`s_invokeWithEc` runs the continuation.** It reads and clears `_realContinuation` / `_realState` / `_capturedEC`. If `_capturedEC` is non-null, it stages cont/state via worker-thread-only scratch fields (`_runCb`, `_runState`) and calls `ExecutionContext.Run(_capturedEC, s_runContinuation, awaiter)`. If null (consumer suppressed flow), it invokes the continuation directly.

In the **`SetResult`-fires-first race** — producer signals before consumer has called `OnCompleted` — `MRVTSC` records the result, finds `_continuation` null, and queues `s_dispatch` to the ThreadPool when `OnCompleted` is later called. The TP-dispatched `s_dispatch` then performs steps 4-5 above. Correctness rests on publication ordering in `OnCompleted`: `_realContinuation` / `_realState` / `_capturedEC` are written via `Volatile.Write` BEFORE `_core.OnCompleted`, so the TP-dispatched `s_dispatch` reads them post-publication.

The two static delegates (per `SpscAwaiter<T>`'s type parameter) are:

- `s_dispatch` — registered with `_core.OnCompleted`; routes through the dispatcher.
- `s_invokeWithEc` — registered with the dispatcher; applies EC and invokes the real continuation.

Plus a `ContextCallback` (`s_runContinuation`) used by `ExecutionContext.Run` inside `s_invokeWithEc` for the allocation-free pattern.

Lost-wakeup re-check paths inside `ParkReadAwaiter` / `ParkFlushAwaiter` that **don't** go through `_core.Set*` (the data-return paths that build a `ValueTask<T>` directly via `BuildReadResult` / `BuildFlushResult`) are *not* dispatched — there is no continuation, the result is returned synchronously to the caller's await machinery.

**Allocation cost.** Zero allocations per dispatch: `s_dispatch` and `s_invokeWithEc` are allocated once at type-init; `_realContinuation` / `_realState` / `_capturedEC` are direct field writes on the existing awaiter object; the dispatcher receives `(Action<object?>, object?)` with `state = awaiter`. The `ExecutionContext.Run` invocation in `s_invokeWithEc` uses worker-thread-only scratch fields (`_runCb` / `_runState`) plus the static `s_runContinuation` `ContextCallback`, so no per-dispatch closure or tuple is allocated. Per-dispatch overhead vs. the prior wiring: one extra virtual call into the dispatcher (~1-2 ns); should be invisible in throughput benchmarks.
```

- [ ] **Step 4: Update I16 (EC capture / restoration discipline) in the consolidated invariants table**

Find this row in the invariants table (around line 1537):

```
| **I16** | **EC capture / restoration discipline.** ExecutionContext capture for await continuations occurs on the consumer's thread at `IValueTaskSource.OnCompleted` time and is stored on `_core` for restoration. EC application happens inside `_core.SetResult` / `_core.SetException` via `ExecutionContext.RunInternal`, regardless of which thread calls those methods. `IContinuationDispatcher` implementations MUST NOT capture or apply an ExecutionContext themselves; their role is purely to route the callback to a thread. Implementations using `ThreadPool.UnsafeQueueUserWorkItem` satisfy this trivially; implementations using `ThreadPool.QueueUserWorkItem` or `Task.Run` violate it. |
```

Replace with:

```
| **I16** | **Source-side EC capture / apply discipline.** ExecutionContext capture for await continuations occurs in `SpscAwaiter.OnCompleted` on the consumer's thread, gated by `FlowExecutionContext`. The capture is stored on the awaiter (`_capturedEC`) and applied in `s_invokeWithEc` via `ExecutionContext.Run` on the dispatcher's chosen thread. `IContinuationDispatcher` implementations MUST NOT capture or apply an ExecutionContext themselves; their role is purely to route the callback to a thread. Implementations using `ThreadPool.UnsafeQueueUserWorkItem` satisfy this trivially; implementations using `ThreadPool.QueueUserWorkItem` or `Task.Run` capture EC redundantly (wasteful, but does not break the consumer's EC because `s_invokeWithEc` applies the source-side captured EC anyway). |
```

- [ ] **Step 5: Update R10 (continuation dispatch rule) in the consolidated rules table**

Find this row in the rules table (around line 1552):

```
| **R10** | **Continuation dispatch.** `_core.RunContinuationsAsynchronously = false`. After winning CAS Pending→Inactive, every signaler / canceler / token-callback / park re-check (both lost-wakeup and lost-cancel) that calls `_core.SetResult` / `_core.SetException` does so via the stash-and-dispatch sequence: stash the result/exception on `_dispatchResult` / `_dispatchException`, call `(_options.ContinuationDispatcher ?? ThreadPoolContinuationDispatcher.Instance).UnsafeQueueUserWorkItem(s_dispatchCallback, awaiter)`, and let the dispatched callback clear the stash and call `_core.Set*`. Sync data returns built directly into a `ValueTask<T>` via `BuildReadResult` / `BuildFlushResult` are exempt (no continuation, no dispatch). |
```

Replace with:

```
| **R10** | **Continuation dispatch.** `_core.RunContinuationsAsynchronously = false`. After winning CAS Pending→Inactive, every signaler / canceler / token-callback / park re-check (both lost-wakeup and lost-cancel) that needs to deliver a result via the awaiter calls `_core.SetResult` / `_core.SetException` directly on the producer's thread. `MRVTSC` invokes the registered `s_dispatch` callback inline (because RCA=false); `s_dispatch` routes through the configured `IContinuationDispatcher`; `s_invokeWithEc` reads the awaiter's source-side-captured `_realContinuation` / `_realState` / `_capturedEC`, applies EC if non-null via `ExecutionContext.Run`, and invokes the user's continuation. Sync data returns built directly into a `ValueTask<T>` via `BuildReadResult` / `BuildFlushResult` are exempt (no continuation, no dispatch). |
```

- [ ] **Step 6: Update §6 IContinuationDispatcher subsection — add the scheduler-bypass clause**

Find this subsection (around lines 1313-1360, starting `### \`IContinuationDispatcher\``). At the end of the subsection (after the `**Why \`Action<object?>\` and \`UnsafeQueueUserWorkItem\`-shaped naming.**` paragraph and before `### Completion overview`), append a new subsection:

```
### Scheduler bypass

`SpscPipe`'s `Reader.ReadAsync` and `Writer.FlushAsync` continuations do **not** honor the consumer's captured `SynchronizationContext` or `TaskScheduler`. The continuation runs on the thread chosen by the configured `IContinuationDispatcher` (default: the .NET `ThreadPool` via `ThreadPoolContinuationDispatcher`). This is independent of the consumer's `ConfigureAwait(true|false)` choice — both produce identical observable behavior. The implementation enforces this by stripping `ValueTaskSourceOnCompletedFlags.UseSchedulingContext` from the flags forwarded to `_core.OnCompleted` in `SpscAwaiter<T>.OnCompleted` (see the source-side EC-capture spec, §2.2 and §3.3).

Consumers requiring continuation on a specific scheduler should either: (a) post explicitly via `SynchronizationContext.Post` / `TaskScheduler.FromCurrentSynchronizationContext().StartNew` after the `await`, or (b) wrap the awaitable in a `Task.Run` to capture context boundaries.

This is a deliberate contract choice, not an implementation accident. `SpscPipe` is a high-throughput primitive aimed at server-side workloads where consumer-side scheduler capture is not the desired routing.
```

- [ ] **Step 7: Verify no remaining references to the old field/delegate names**

Run:

```bash
grep -n '_dispatchResult\|_dispatchException\|s_dispatchReadSetResult\|s_dispatchReadSetException\|s_dispatchFlushSetResult\|s_dispatchFlushSetException\|s_dispatchCallback' docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md
```

Expected: no matches.

The `RunInternal` / "MRVTSC restores EC" passages in the SPSC pipe spec are now potentially misleading. Run:

```bash
grep -n 'RunInternal\|MRVTSC.*restor' docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md
```

If any matches exist that describe MRVTSC.RunInternal as the EC mechanism, edit them to point to the source-side EC-capture spec instead. (One known location is in §5's I16 if it was missed in step 4. Re-read I16 to confirm. Another may be in the "SynchronizationContext / TaskScheduler interaction" paragraph at the end of §5 "Continuation dispatch"; replace its content with: "See §3.3 of the source-side EC-capture spec for the public contract — both SC and TaskScheduler captured by the consumer's await are stripped from the flags forwarded to `_core.OnCompleted`, so the dispatcher's chosen thread is always honored.")

- [ ] **Step 8: Commit**

```bash
git add docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md
git commit -m "$(cat <<'EOF'
SpscPipe spec: revise §5 / §6 / I16 / R10 for source-side EC capture

Updates the SpscPipe design spec to match the source-side EC-capture
implementation:
- §5 awaiter shape: replace _dispatchResult/_dispatchException fields
  with _realContinuation/_realState/_capturedEC.
- §5 field-access discipline: rewrite the dispatch-stash bullet to
  describe consumer-thread writes / dispatcher-thread reads with the
  Volatile.Write publication chain.
- §5 "Continuation dispatch": rewrite the protocol from "stash on
  _dispatch{Result,Exception} + DispatchVia(s_dispatchXxx, awaiter)"
  to "direct _core.SetResult/SetException calls; s_dispatch (registered
  via OnCompleted) routes inline through the dispatcher; s_invokeWithEc
  applies the source-captured EC on the dispatcher's chosen thread."
  Allocation cost paragraph updated to reflect the new pattern.
- §6 IContinuationDispatcher: add the new "Scheduler bypass" subsection
  documenting the public contract that SC/TaskScheduler set at the
  await site are NOT honored.
- I16: rewrite from "MRVTSC.RunInternal applies the captured EC" to
  "SpscAwaiter.OnCompleted captures EC; s_invokeWithEc applies via
  ExecutionContext.Run".
- R10: rewrite to describe the new direct-SetResult / s_dispatch /
  s_invokeWithEc flow.

Spec ref: source-side EC-capture spec §2, §3.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Update HotHandoff dispatcher spec — §6 EC contract

Per spec §3.4: the HotHandoff dispatcher spec's §6 "EC contract" needs revision to match the new flow. The HotHandoff dispatcher does not directly capture or apply EC (this remains true); the EC is now stored on the work item (passed via `awaiter` as `state`) and applied by the dispatcher's chosen thread when invoking `s_invokeWithEc`. The four-races correctness argument (§5) is unaffected — it concerns slot/dispatch-state synchronization, not EC.

**Files:**
- Modify: `docs/superpowers/specs/2026-04-27-hot-handoff-dispatcher-design.md`

- [ ] **Step 1: Replace §6 in full**

Find the entire `## Section 6 — \`ExecutionContext\` contract` section (lines 193-202, ending just before `## Section 7 — Required test surface`). Replace with:

```
## Section 6 — `ExecutionContext` contract

Restated for this implementation:

The `IContinuationDispatcher` contract item #2 forbids EC capture in the dispatcher. This is a structural property of the implementation:

- **Slot path.** The dispatcher writes `callback` (the un-wrapped `Action<object?>` passed to `UnsafeQueueUserWorkItem` — concretely, `SpscAwaiter<T>.s_invokeWithEc`) directly into `_pending`, paired with `state` (the awaiter, which carries the source-side-captured EC and the user's continuation). The dedicated thread reads them and invokes the callback directly. No closure is allocated, no EC primitive is touched on the dispatcher side — the EC is captured by `SpscAwaiter` on the consumer's thread (per the source-side EC-capture spec) and applied by `s_invokeWithEc` via `ExecutionContext.Run` on the dedicated thread. The dedicated thread's own EC is saved and restored by `ExecutionContext.Run` around the continuation invocation, so AsyncLocal state on the dispatcher thread is **isolated from** the continuation, not leaked into it.
- **Overflow path.** The dispatcher forwards to `ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false)` — the unsafe variant, which does not capture EC. The same `s_invokeWithEc`-applies-source-captured-EC chain runs on whatever TP worker picks up the work item.

Both paths satisfy contract item #2 without dispatcher-side EC manipulation. EC correctness — including cross-tenant isolation when `FlowExecutionContext` is suppressed — is the responsibility of `SpscAwaiter<T>` (the source) per the source-side EC-capture spec; the dispatcher's role is purely to route work items to threads.

The existing tests in `tests/SpscPipe.Tests/SpscPipeContinuationDispatcherTests.cs` (specifically `CustomDispatcher_AsyncLocalFlowsToContinuation`, `CustomDispatcher_DispatcherThreadAsyncLocal_NotObservedInContinuation`, `CustomDispatcher_DispatcherThreadAsyncLocal_RestoredAfterContinuation`, `MultiCycle_PerCycleEcCapture_AppliesCorrectEcEachCycle`, `SuppressFlow_AtAwait_NoCapturedEC_BranchExercisedCleanly`) pin the EC behavior at the SpscPipe level for any conforming dispatcher; the corresponding tests in this project (`SpscPipe_WithHotHandoff_AsyncLocalFlowsToContinuation`, `SpscPipe_WithHotHandoff_DispatcherThreadAsyncLocal_NotObservedInContinuation`) pin it specifically through `HotHandoffContinuationDispatcher`.
```

- [ ] **Step 2: Update Section 11 spec references — add cross-reference to the new spec**

Find the `## Section 11 — Spec references` section (around line 275). Add a new bullet at the top of the references list:

```
- `docs/superpowers/specs/2026-04-28-spsc-awaiter-source-side-ec-capture-design.md` — source-side EC capture spec. Section 6 above is consistent with that spec's §3.4; the four-races argument in §5 remains unaffected (EC handling is orthogonal to the slot/dispatch-state synchronization).
```

- [ ] **Step 3: Verify no remaining references to MRVTSC.RunInternal**

Run:

```bash
grep -n 'RunInternal\|MRVTSC.*restor\|MRVTSC.*captures\|MRVTSC.*applies' docs/superpowers/specs/2026-04-27-hot-handoff-dispatcher-design.md
```

If any matches exist that describe `MRVTSC.RunInternal` as the EC mechanism in any section (other than as historical context), edit them to point to the source-side EC-capture flow.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-04-27-hot-handoff-dispatcher-design.md
git commit -m "$(cat <<'EOF'
HotHandoff spec: revise §6 EC contract for source-side capture

The HotHandoff dispatcher's EC contract is unchanged in spirit — it
still does NOT capture or apply EC itself — but the description of
WHY the contract holds was outdated: it referenced MRVTSC.RunInternal
applying the consumer's captured EC, which is no longer what happens.

Updated §6 to describe the new flow:
- Slot path: dispatcher writes s_invokeWithEc + awaiter into the slot;
  worker thread invokes; SpscAwaiter applies the source-side-captured
  EC via ExecutionContext.Run.
- Overflow path: ThreadPool.UnsafeQueueUserWorkItem forwards same
  (callback, state) pair; same s_invokeWithEc-applies-EC chain on TP
  worker.

EC isolation guarantees (consumer's AsyncLocal observed; dispatcher
thread's AsyncLocal NOT observed; dispatcher thread's AsyncLocal
restored after continuation) all remain — they're now provided by
ExecutionContext.Run inside s_invokeWithEc rather than by
MRVTSC.RunInternal.

Section 11 (Spec references): added cross-reference to the new
source-side EC-capture spec.

Spec ref: source-side EC-capture spec §3.4.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Final verification — solution build, full test suite, no regressions

Confirms the entire stack is in a consistent state: source code, tests, and docs all agree on the new architecture.

- [ ] **Step 1: Clean build**

Run:

```bash
dotnet build SpscPipe.slnx -c Debug
```

Expected: 6 projects build, 0 warnings, 0 errors.

```bash
dotnet build SpscPipe.slnx -c Release
```

Expected: 6 projects build (Release), 0 warnings, 0 errors.

- [ ] **Step 2: Run all tests, both configurations**

Run:

```bash
dotnet test SpscPipe.slnx -c Debug --nologo
```

Expected: all tests pass (the existing test count + 7 new tests added by Tasks 2-7 — `MultiCycle_PerCycleEcCapture_AppliesCorrectEcEachCycle`, `SuppressFlow_AtAwait_NoCapturedEC_BranchExercisedCleanly`, `SynchronizationContext_AtAwait_NotHonored_ContinuationOnDispatcherThread`, `TaskScheduler_AtAwait_NotHonored_ContinuationOnDispatcherThread`, two `ConfigureAwait_TrueAndFalse_BothRunOnDispatcherThread` cases (Theory with `[InlineData(true)]` / `[InlineData(false)]`), and `SetResultBeforeOnCompleted_DirectAwaiter_Race_StressN_AllResultsDelivered`).

```bash
dotnet test SpscPipe.slnx -c Release --nologo
```

Expected: same — all tests pass under Release optimizations.

- [ ] **Step 3: Confirm the obsolete code/types are fully gone**

Run:

```bash
grep -RIn '_dispatchResult\|_dispatchException\|s_dispatchReadSetResult\|s_dispatchReadSetException\|s_dispatchFlushSetResult\|s_dispatchFlushSetException\|DispatchVia' src/ tests/ docs/
```

Expected: no matches in source/tests/docs (the old delegates / helper / fields have no leftover references). Some matches MAY remain in `docs/superpowers/plans/2026-04-27-hot-handoff-dispatcher-implementation.md` (a historical plan document; do not modify) and in old commit messages — those are expected and not regressions.

- [ ] **Step 4: Confirm key new pieces are present**

Run:

```bash
grep -n 'private static readonly Action<object?> s_dispatch \|private static readonly Action<object?> s_invokeWithEc \|private static readonly ContextCallback s_runContinuation' src/SpscPipelines/SpscAwaiter.cs
```

Expected: 3 matches (one per delegate).

Run:

```bash
grep -n 'public Action<object?>? _realContinuation\|public object? _realState\|public ExecutionContext? _capturedEC' src/SpscPipelines/SpscAwaiter.cs
```

Expected: 3 matches (one per field).

Run:

```bash
grep -n 'public SpscAwaiter(IContinuationDispatcher' src/SpscPipelines/SpscAwaiter.cs
```

Expected: 1 match (the constructor).

Run:

```bash
grep -nE 'flags & ~suppressed|UseSchedulingContext' src/SpscPipelines/SpscAwaiter.cs
```

Expected: at least 2 matches showing the `UseSchedulingContext` flag stripping in `OnCompleted`.

- [ ] **Step 5: Confirm the HotHandoff benchmark file is in its restored throughput-shape**

Run:

```bash
grep -E 'TotalBytes|ChunkSize|chunk\.CopyTo|new byte\[ChunkSize\]' tests/SpscPipelines.HotHandoff.Benchmarks/DispatcherThroughputBench.cs
```

Expected output:

```
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize  = 4096;
            var chunk = new byte[ChunkSize];
                chunk.CopyTo(memory);
```

(This re-verifies Task 1's revert survived through Tasks 2-10.)

- [ ] **Step 6: Smoke-run a small benchmark to confirm no regression in a quick sanity check**

Run:

```bash
dotnet run -c Release --project tests/SpscPipe.Benchmarks --nologo -- latency --count 10000 --size 256 --trials 1
```

Expected: completes in < 30 seconds, prints comparison stats. No crashes / hangs / unexpected exceptions. Exact numbers are not part of the verification — they're characterization data.

- [ ] **Step 7: Inspect the commit chain**

Run:

```bash
git log --oneline -15
```

Expected: shows the chain of plan-implementing commits (one per task), most recent first:

```
<sha> HotHandoff spec: revise §6 EC contract for source-side capture
<sha> SpscPipe spec: revise §5 / §6 / I16 / R10 for source-side EC capture
<sha> docs: revise IContinuationDispatcher contract for source-side EC capture
<sha> SpscAwaiter tests: E.1 — SetResult-fires-first race (N-iteration stress)
<sha> SpscAwaiter tests: D.3 — ConfigureAwait(true/false) parity
<sha> SpscAwaiter tests: D.2 — TaskScheduler capture bypassed
<sha> SpscAwaiter tests: D.1 — SynchronizationContext capture bypassed
<sha> SpscAwaiter tests: C.3 — FlowExecutionContext suppression branch
<sha> SpscAwaiter: source-side EC capture (OpenTcp pattern)
<sha> HotHandoff bench: revert TEMP commits c1ba6b9 / 99293c1 / 02a6dd1
<earlier history>
```

No additional commit is needed for this task — Tasks 1-10 each committed their changes. Implementation is complete; the branch is ready for review and a merge to main.
