using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks.Sources;

namespace Pipely;

internal sealed class PipelyAwaiter<T> : IValueTaskSource<T>, IThreadPoolWorkItem
{
    // IThreadPoolWorkItem.Execute lets us be queued via
    // ThreadPool.UnsafeQueueUserWorkItem(this, ...) — zero per-signal allocation,
    // because the awaiter itself is the work-item node. Used only by the TP
    // fast-path inside s_dispatch (when _dispatcher is PipeScheduler.ThreadPool);
    // other dispatchers continue to go through PipeScheduler.Schedule(action, state).
    void IThreadPoolWorkItem.Execute() => s_invokeWithEc(this);

    // RCA = false: with source-side EC capture, every signal-path SetResult/SetException
    // invokes our registered s_dispatch INLINE on the producer thread (RCA=false ⇒ MRVTSC
    // runs the registered callback synchronously on the calling thread). s_dispatch then
    // routes the work item through the configured PipeScheduler. With RCA=true,
    // MRVTSC would queue s_dispatch to the ThreadPool itself before invoking it — adding
    // a redundant TP hop and breaking the dispatcher's thread-routing guarantee. See spec
    // §2.5. RCA=false is set once at construction; ManualResetValueTaskSourceCore<T>.Reset
    // does NOT reset this flag, so it remains correct across park cycles.
    public ManualResetValueTaskSourceCore<T> _core = new() { RunContinuationsAsynchronously = false };
    public int _state;
    public CancellationTokenRegistration _ctr;
    public CancellationToken _token;

    // Pattern 2 stash (used by PipelyAwaiter<ReadResult>; ignored by PipelyAwaiter<FlushResult>).
    public BufferSegment? _stashHead;
    public int _stashHeadIdx;
    public BufferSegment? _stashTail;
    public int _stashTailIdx;

    // Source-side EC-capture stash. Written by OnCompleted on the consumer's thread BEFORE
    // delegating to _core.OnCompleted (so visible by the time s_dispatch reads them — including
    // in the SetResult-fires-first race; see spec §4 publication ordering). Read by
    // s_invokeWithEc on the dispatcher's chosen thread, which applies _capturedEC via
    // ExecutionContext.Run if non-null and invokes _realContinuation(_realState).
    private Action<object?>? _realContinuation;
    private object? _realState;
    private ExecutionContext? _capturedEC;

    // Worker-thread-only scratch fields used by the allocation-free ExecutionContext.Run pattern
    // (spec §5). s_invokeWithEc writes _runCb/_runState before ExecutionContext.Run; s_runContinuation
    // reads them and clears them. Only the dispatcher's chosen thread accesses these, sequentially
    // around each Run invocation, so plain reads/writes are sufficient — no concurrent writers.
    private Action<object?>? _runCb;
    private object? _runState;

    // The scheduler this awaiter routes continuations through. Set once at construction;
    // immutable for the awaiter's lifetime. Stored on the awaiter so s_dispatch can reach it
    // without a back-pointer to Pipe.
    private readonly PipeScheduler _dispatcher;

    // When true, OnCompleted captures a non-default SynchronizationContext from the consumer
    // thread; s_dispatch then routes the continuation via SC.Post instead of _dispatcher.Schedule.
    // Set once at construction from PipeOptions.UseSynchronizationContext.
    private readonly bool _useSyncContext;

    // Source-side SC capture stash. Written in OnCompleted on the consumer thread (with
    // Volatile.Write, ordered alongside _capturedEC / _realState / _realContinuation). Read by
    // s_dispatch with a plain read; the publication happens-before edge is provided by MRVTSC's
    // interlocked-on-_continuation (OnCompleted-fires-first path) or by the TP queue→dequeue
    // (SetResult-fires-first race). Cleared in s_invokeWithEc on the dispatcher's thread.
    private SynchronizationContext? _capturedSC;

    public const int Inactive   = 0b00;
    public const int Pending    = 0b01;
    public const int StateMask  = 0b01;
    public const int CancelFlag = 0b10;

    // Diagnostic counters (Interlocked-incremented at each CAS resolution site). Cost ~5-10 ns
    // per increment, only on park/signal paths (off the synchronous hot path). Read by the
    // benchmark project (`tests/Pipely.Benchmarks/PipeAdapter.cs`) after a run completes;
    // unrelated to the EC-capture work.
    public long _parkCount;
    public long _signalWonCount;
    public long _tokenCancelWonCount;
    public long _cancelPendingWonCount;
    public long _lostWakeupResolvedCount;
    public long _lostCancelResolvedCount;

    public PipelyAwaiter(PipeScheduler dispatcher, bool useSynchronizationContext = false)
    {
        _dispatcher     = dispatcher;
        _useSyncContext = useSynchronizationContext;
    }

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

        // Capture SynchronizationContext on the awaiter thread (the consumer's), under the same
        // rationale as EC capture — capturing inside s_dispatch would observe the producer
        // thread's SC, which is not what the consumer's await semantics promise. The SC is
        // captured iff (a) the pipe was constructed with UseSynchronizationContext = true,
        // (b) the consumer didn't suppress UseSchedulingContext in the await flags
        // (e.g. ConfigureAwait(false) strips it), and (c) SC.Current is non-default
        // (non-null and not exactly the base SynchronizationContext type — matches BCL's
        // PipeAwaitable.OnCompleted check at runtime/PipeAwaitable.cs:115-127).
        SynchronizationContext? sc = null;
        if (_useSyncContext &&
            (flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) != 0)
        {
            var current = SynchronizationContext.Current;
            if (current is not null && current.GetType() != typeof(SynchronizationContext))
                sc = current;
        }

        // Volatile.Write publication order: cap EC, cap SC, then state, then continuation. The
        // TP-dispatched s_dispatch in the SetResult-fires-first race reads these post-publication
        // via the happens-before edge from queue-call to dequeued callback. See spec §4.
        Volatile.Write(ref _capturedEC, ec);
        Volatile.Write(ref _capturedSC, sc);
        Volatile.Write(ref _realState, state);
        Volatile.Write(ref _realContinuation, continuation);

        // Strip both EC and SchedulingContext flags before forwarding. EC is captured by us;
        // leaving the flag on would have MRVTSC capture again (wasteful, unused). SchedulingContext
        // is stripped because routing is decided by us in s_dispatch (either through the captured
        // SC or through the configured dispatcher); we don't want MRVTSC to also try to honor it.
        // See spec §2.2 and §3.3.
        const ValueTaskSourceOnCompletedFlags suppressed =
            ValueTaskSourceOnCompletedFlags.FlowExecutionContext |
            ValueTaskSourceOnCompletedFlags.UseSchedulingContext;
        _core.OnCompleted(s_dispatch, this, token, flags & ~suppressed);
    }

    // Registered with _core via OnCompleted. Invoked inline by MRVTSC.SetResult on the producer
    // thread (RCA=false), or — in the rare SetResult-fires-first race — queued to TP by MRVTSC
    // and invoked there. In either case, routes the work item (the awaiter itself, as state)
    // through the captured SC if present, else through the configured dispatcher. Either path
    // ends in s_invokeWithEc, which applies the captured EC and invokes the consumer's continuation.
    private static readonly Action<object?> s_dispatch = static state =>
    {
        var awaiter = (PipelyAwaiter<T>)state!;
        var sc = awaiter._capturedSC;
        if (sc is not null)
        {
            sc.Post(s_invokeWithEcSendOrPost!, awaiter);
        }
        else if (ReferenceEquals(awaiter._dispatcher, PipeScheduler.ThreadPool))
        {
            // TP fast-path: queue the awaiter itself as the work item. Avoids the
            // wrapper allocation that PipeScheduler.ThreadPool.Schedule(Action<object?>,
            // object) would otherwise pay per signal — the same trick BCL Pipe uses.
            ThreadPool.UnsafeQueueUserWorkItem(awaiter, preferLocal: false);
        }
        else
        {
            awaiter._dispatcher.Schedule(s_invokeWithEc!, awaiter);
        }
    };

    // SendOrPostCallback adapter for the SC.Post path. Forwards to s_invokeWithEc, which has
    // the (object? state) signature already; SendOrPostCallback's signature is identical
    // (it's also `void(object?)`), so the adapter is just a static delegate cache to avoid
    // re-allocating per dispatch.
    private static readonly SendOrPostCallback s_invokeWithEcSendOrPost = static state => s_invokeWithEc!(state);

    // Invoked by the scheduler's chosen thread (FastScheduler worker, TP worker for overflow, or
    // TP for PipeScheduler.ThreadPool). Reads the awaiter's fields, clears them, applies
    // the consumer-captured EC if any, and invokes the continuation.
    private static readonly Action<object?> s_invokeWithEc = static state =>
    {
        var awaiter = (PipelyAwaiter<T>)state!;
        var cont = awaiter._realContinuation;
        var st   = awaiter._realState;
        var ec   = awaiter._capturedEC;
        awaiter._realContinuation = null;
        awaiter._realState = null;
        awaiter._capturedEC = null;
        awaiter._capturedSC = null;

        if (ec is not null)
        {
            // Allocation-free pattern (spec §5): pass the awaiter as state to ExecutionContext.Run,
            // staging cont/st via worker-thread-only scratch fields. The static ContextCallback
            // reads cont/st and clears the scratch fields on the worker thread.
            awaiter._runCb = cont;
            awaiter._runState = st;
            ExecutionContext.Run(ec, s_runContinuation!, awaiter);
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
        var awaiter = (PipelyAwaiter<T>)state!;
        var cb = awaiter._runCb!;
        var st = awaiter._runState;
        awaiter._runCb = null;
        awaiter._runState = null;
        cb(st);
    };
}
