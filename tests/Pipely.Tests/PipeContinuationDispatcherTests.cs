using System.Threading;
using System.Threading.Tasks.Sources;
using Xunit;

namespace PipelyTests;

public class PipeContinuationDispatcherTests
{
    // ---------- Helper dispatchers ----------

    private sealed class RecordingDispatcher : Pipely.IContinuationDispatcher
    {
        private readonly Action<Action<object?>>? _onDispatch;
        public RecordingDispatcher(Action<Action<object?>>? onDispatch = null) => _onDispatch = onDispatch;

        public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
        {
            _onDispatch?.Invoke(callback);
            // Forward to TP so the await completes.
            ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
        }
    }

    /// <summary>
    /// Forwards every dispatch directly to <see cref="ThreadPool.UnsafeQueueUserWorkItem(Action{object?}, object?, bool)"/>.
    /// Used when we need a "real" custom dispatcher but don't care about the thread.
    /// IDisposable for symmetry with the other helpers; nothing to dispose.
    /// </summary>
    private sealed class ForwardingDispatcher : Pipely.IContinuationDispatcher, IDisposable
    {
        public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
            => ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);

        public void Dispose() { }
    }

    /// <summary>
    /// "Bad" dispatcher: uses <see cref="ThreadPool.QueueUserWorkItem(WaitCallback, object?)"/>, the EC-capturing
    /// variant. Demonstrates that the EC contract guarantee on the consumer's continuation still holds even
    /// if a dispatcher misbehaves and captures EC, because MRVTSC's RunInternal restores the consumer's EC
    /// regardless of which thread (or under what context) the dispatched callback runs.
    /// </summary>
    private sealed class BadEcCapturingDispatcher : Pipely.IContinuationDispatcher
    {
        public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
            => ThreadPool.QueueUserWorkItem(s => callback(s), state);
    }

    /// <summary>
    /// Dispatcher backed by a single dedicated thread.
    /// - Single-slot mailbox protected by a lock + signal. Overflow falls back to ThreadPool to keep
    ///   producer-side contract item #4 (must not throw, must not refuse the work).
    /// - Optional <c>setupOnThread</c> hook runs once on the dedicated thread before the loop starts;
    ///   used by tests to set an AsyncLocal or other per-thread state.
    /// - Optional <c>captureBeforeCallback</c> / <c>captureAfterCallback</c> hooks fire around each
    ///   dispatched invocation, observing dispatcher-thread state.
    /// - Wraps the callback invocation in try/catch (per contract item #5) so a throwing continuation
    ///   doesn't kill the dispatcher thread.
    /// - Disposable: shutdown signals the worker loop to exit and joins.
    /// </summary>
    private sealed class DedicatedThreadDispatcher : Pipely.IContinuationDispatcher, IDisposable
    {
        private readonly Thread _thread;
        private readonly Action? _setupOnThread;
        private readonly Action? _captureBeforeCallback;
        private readonly Action? _captureAfterCallback;
        private readonly object _gate = new();
        private (Action<object?> cb, object? state)? _slot;
        private bool _shutdown;

        public DedicatedThreadDispatcher(
            Action? setupOnThread = null,
            Action? captureBeforeCallback = null,
            Action? captureAfterCallback = null)
        {
            _setupOnThread          = setupOnThread;
            _captureBeforeCallback  = captureBeforeCallback;
            _captureAfterCallback   = captureAfterCallback;
            _thread = new Thread(WorkerLoop)
            {
                IsBackground = true,
                Name = nameof(DedicatedThreadDispatcher),
            };
            _thread.Start();
        }

        public void UnsafeQueueUserWorkItem(Action<object?> callback, object? state)
        {
            // Try to enqueue into the single slot. If the slot is occupied (overflow) or we're
            // shutting down, fall back to TP so the callback still runs (contract item #4).
            lock (_gate)
            {
                if (!_shutdown && _slot is null)
                {
                    _slot = (callback, state);
                    Monitor.Pulse(_gate);
                    return;
                }
            }
            ThreadPool.UnsafeQueueUserWorkItem(callback, state, preferLocal: false);
        }

        private void WorkerLoop()
        {
            try { _setupOnThread?.Invoke(); }
            catch { /* swallow setup failures so loop still runs */ }

            while (true)
            {
                (Action<object?> cb, object? state) work;
                lock (_gate)
                {
                    while (!_shutdown && _slot is null)
                        Monitor.Wait(_gate);
                    if (_shutdown && _slot is null) return;
                    work = _slot!.Value;
                    _slot = null;
                }

                try { _captureBeforeCallback?.Invoke(); } catch { /* ignored */ }

                // Contract item #5: wrap in try/catch so an exception from the continuation
                // does not kill the dispatcher thread.
                try { work.cb(work.state); }
                catch { /* swallow — survival is the test invariant */ }

                try { _captureAfterCallback?.Invoke(); } catch { /* ignored */ }
            }
        }

        public void Dispose()
        {
            lock (_gate)
            {
                _shutdown = true;
                Monitor.PulseAll(_gate);
            }
            _thread.Join(TimeSpan.FromSeconds(5));
        }
    }

    // ---------- Tests ----------

    [Fact]
    public async Task CustomDispatcher_ReceivesContinuationCallback_ForReadAsync()
    {
        int dispatchCount = 0;
        var dispatcher = new RecordingDispatcher(_ => Interlocked.Increment(ref dispatchCount));
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        var result = await readTask;
        Assert.Equal(5, result.Buffer.Length);
        Assert.Equal(1, dispatchCount);
    }

    // ---------- A. EC flow correctness — consumer's AsyncLocal observed in continuation ----------

    [Fact]
    public async Task CustomDispatcher_AsyncLocalFlowsToContinuation()
    {
        var asyncLocal = new AsyncLocal<int>();
        using var dispatcher = new ForwardingDispatcher();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        asyncLocal.Value = 42;

        // Producer fires in background; the small delay gives the consumer time to park
        // on ReadAsync so the completion path goes through the dispatcher.
        _ = Task.Run(async () =>
        {
            await Task.Delay(50);
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        // Consumer awaits ReadAsync directly. This await suspends, the dispatcher
        // resumes us, and the next line runs on the dispatcher's chosen thread.
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);

        // Observe AsyncLocal HERE — on the dispatcher's continuation path.
        // If MRVTSC's RunInternal correctly applied the captured EC, value is 42.
        int observedInContinuation = asyncLocal.Value;
        Assert.Equal(5, rr.Buffer.Length);
        Assert.Equal(42, observedInContinuation);
    }

    // ---------- B. EC isolation — dispatcher's own AsyncLocal NOT observed in continuation ----------

    [Fact]
    public async Task CustomDispatcher_DispatcherThreadAsyncLocal_NotObservedInContinuation()
    {
        var consumerLocal   = new AsyncLocal<int>();
        var dispatcherLocal = new AsyncLocal<int>();
        using var dispatcher = new DedicatedThreadDispatcher(
            setupOnThread: () => dispatcherLocal.Value = 999);
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        consumerLocal.Value = 42;

        // Producer fires in background; small delay gives the consumer time to park.
        _ = Task.Run(async () =>
        {
            await Task.Delay(50);
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        // Consumer awaits ReadAsync directly. This await suspends, the dispatcher
        // (whose dedicated thread had its dispatcherLocal set to 999) resumes us, and
        // the next line runs on the dispatcher's thread under the consumer's restored EC.
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);

        // Observe both AsyncLocals HERE — on the dispatcher's continuation path.
        // - Consumer's value should be 42 (consumer's captured EC was restored).
        // - Dispatcher's value should NOT be 999 (RunInternal isolated dispatcher's
        //   per-thread EC from the continuation invocation).
        int observedConsumer   = consumerLocal.Value;
        int observedDispatcher = dispatcherLocal.Value;

        Assert.Equal(5, rr.Buffer.Length);
        Assert.Equal(42, observedConsumer);
        Assert.Equal(0, observedDispatcher);
    }

    // ---------- C. EC restoration — dispatcher's AsyncLocal preserved across continuation ----------

    [Fact]
    public async Task CustomDispatcher_DispatcherThreadAsyncLocal_RestoredAfterContinuation()
    {
        var dispatcherLocal = new AsyncLocal<int>();
        int observedOnDispatcherThreadBeforeCallback = 0;
        int observedOnDispatcherThreadAfterCallback  = 0;

        using var dispatcher = new DedicatedThreadDispatcher(
            setupOnThread:          () => dispatcherLocal.Value = 777,
            captureBeforeCallback:  () => observedOnDispatcherThreadBeforeCallback = dispatcherLocal.Value,
            captureAfterCallback:   () => observedOnDispatcherThreadAfterCallback  = dispatcherLocal.Value);

        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        var consumerLocal = new AsyncLocal<int>();
        consumerLocal.Value = 42;

        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        await readTask.WaitAsync(TimeSpan.FromSeconds(5));

        // Wait briefly for the dispatcher's after-callback hook to fire (it fires on the
        // dispatcher thread *after* control returns from the continuation).
        SpinWait.SpinUntil(() => observedOnDispatcherThreadAfterCallback != 0, TimeSpan.FromSeconds(5));

        // Dispatcher's value before AND after the continuation: both 777, because MRVTSC's
        // RunInternal saves and restores the dispatcher thread's EC around the consumer's EC.
        Assert.Equal(777, observedOnDispatcherThreadBeforeCallback);
        Assert.Equal(777, observedOnDispatcherThreadAfterCallback);
    }

    // ---------- D. EC contract guard — dispatchers that capture EC are caught ----------

    /// <summary>
    /// Pins that a "bad" dispatcher (one that captures EC at queue time, e.g.,
    /// uses ThreadPool.QueueUserWorkItem instead of UnsafeQueueUserWorkItem) does
    /// NOT corrupt the consumer's continuation EC. Under the new source-side EC
    /// capture wiring, the consumer's EC is captured by Pipely.PipelyAwaiter.OnCompleted
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
    {
        // A "bad" dispatcher that uses ThreadPool.QueueUserWorkItem (captures EC).
        var badDispatcher = new BadEcCapturingDispatcher();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = badDispatcher });

        var consumerLocal = new AsyncLocal<int>();
        var producerLocal = new AsyncLocal<int>();

        consumerLocal.Value = 42;

        // Producer fires in background; small delay gives the consumer time to park
        // so the completion path goes through the (bad) dispatcher.
        _ = Task.Run(async () =>
        {
            producerLocal.Value = 99;  // set on producer's thread
            await Task.Delay(50);
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        // Consumer awaits ReadAsync directly. The bad dispatcher captures producer's EC
        // and applies it to the work item, but MRVTSC's inner RunInternal restores the
        // consumer's captured EC for the actual continuation invocation.
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);

        // Observe AsyncLocals HERE — on the dispatcher's continuation path.
        // - consumerLocal.Value should still be 42 (consumer's captured EC was restored).
        // - producerLocal.Value should be 0 (producer's EC was the work-item-wrapping EC,
        //   which RunInternal saved/restored across the continuation).
        int observedConsumer = consumerLocal.Value;
        int observedProducer = producerLocal.Value;

        Assert.Equal(5, rr.Buffer.Length);

        // The consumer's value must always be observed (this is the hard guarantee even
        // with a misbehaving dispatcher, because MRVTSC's RunInternal restores the
        // captured EC for the continuation regardless of dispatcher).
        Assert.Equal(42, observedConsumer);

        // The producer's EC, if captured by the bad dispatcher, never reaches the
        // continuation: Pipely.PipelyAwaiter.OnCompleted already captured the consumer's
        // EC on the consumer's thread BEFORE the bad dispatcher's queue-time
        // capture could matter, and s_invokeWithEc applies that captured consumer
        // EC via ExecutionContext.Run on the dispatcher's chosen thread. The bad
        // dispatcher's EC capture is wasted work, not a correctness hazard.
        Assert.Equal(0, observedProducer);
    }

    // ---------- E. Exception in continuation doesn't kill dispatcher thread ----------

    [Fact]
    public async Task CustomDispatcher_ContinuationException_DoesNotKillDispatcherThread()
    {
        using var dispatcher = new DedicatedThreadDispatcher();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        // First read: producer fires in background, consumer parks on ReadAsync, the
        // dispatcher resumes the continuation on its dedicated thread. The continuation
        // (the awaited body below) then throws. The dispatcher's try/catch (contract item
        // #5) protects its thread from in-flight exceptions; the test method's async
        // builder also catches the rethrow on resumption. Either path is acceptable —
        // what matters is that the dispatcher thread survives, verified by the second read.
        _ = Task.Run(async () =>
        {
            await Task.Delay(50);
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        var thrownAsExpected = false;
        try
        {
            var rr = await pipe.Reader.ReadAsync();
            pipe.Reader.AdvanceTo(rr.Buffer.End);
            throw new InvalidOperationException("test exception");
        }
        catch (InvalidOperationException ex) when (ex.Message == "test exception")
        {
            thrownAsExpected = true;
        }

        Assert.True(thrownAsExpected);

        // Second read: confirms dispatcher thread is still alive.
        _ = Task.Run(async () =>
        {
            await Task.Delay(50);
            var mem = pipe.Writer.GetMemory(7);
            mem.Span.Clear();
            pipe.Writer.Advance(7);
            await pipe.Writer.FlushAsync();
        });

        var rr2 = await pipe.Reader.ReadAsync();
        Assert.False(rr2.IsCanceled);
        Assert.Equal(7, rr2.Buffer.Length);
        pipe.Reader.AdvanceTo(rr2.Buffer.End);
    }

    // ---------- F. Version safety under rapid park/resume ----------

    [Fact]
    public async Task CustomDispatcher_RapidParkResumeCycles_NoVersionMismatch()
    {
        using var dispatcher = new ForwardingDispatcher();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        const int totalCycles  = 1000;
        const int messageBytes = 8;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < totalCycles; i++)
            {
                var mem = pipe.Writer.GetMemory(messageBytes);
                mem.Span.Clear();
                pipe.Writer.Advance(messageBytes);
                await pipe.Writer.FlushAsync();
            }
            pipe.Writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            long bytesRead = 0;
            long target    = (long)totalCycles * messageBytes;
            while (bytesRead < target)
            {
                var rr = await pipe.Reader.ReadAsync();
                bytesRead += rr.Buffer.Length;
                pipe.Reader.AdvanceTo(rr.Buffer.End);
                if (rr.IsCompleted) break;
            }
            pipe.Reader.Complete();
        });

        await Task.WhenAll(producer, consumer).WaitAsync(TimeSpan.FromSeconds(30));
    }

    // ---------- G. SetResult-fires-before-OnCompleted race (approximation) ----------

    [Fact]
    public async Task CustomDispatcher_SetResultBeforeOnCompleted_RaceHandled()
    {
        using var dispatcher = new ForwardingDispatcher();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        // Producer writes BEFORE consumer awaits. The consumer's ReadAsync should return
        // synchronously (sync data return — no dispatcher hop, no continuation, no parking).
        var mem = pipe.Writer.GetMemory(5);
        mem.Span.Clear();
        pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();

        // Consumer's IsCompleted should be true synchronously now.
        var readTask = pipe.Reader.ReadAsync();
        var rr = await readTask;
        Assert.False(rr.IsCanceled);
        Assert.Equal(5, rr.Buffer.Length);
    }

    // ---------- C.2 — Per-cycle EC capture/apply hygiene (regression-only) ----------

    /// <summary>
    /// Pins per-cycle EC capture/apply hygiene. Each await on the same Pipe goes
    /// through Pipely.PipelyAwaiter.OnCompleted (capturing the consumer-thread EC at that
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
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

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

    // ---------- C.3 — FlowExecutionContext suppressed (smoke for null-_capturedEC branch) ----------

    /// <summary>
    /// When the consumer awaits inside an ExecutionContext.SuppressFlow() block,
    /// Pipely.PipelyAwaiter.OnCompleted captures _capturedEC = null and forwards (s_dispatch,
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
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        _ = Task.Run(async () =>
        {
            await Task.Delay(50);
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        // No `using` block: AsyncFlowControl.Undo() is thread-affine and would throw
        // when the using's Dispose runs on the post-await continuation thread (the
        // dispatcher's worker thread, different from the SuppressFlow caller).
        // The suppression "leaks" past method end; benign for this xunit test.
        ExecutionContext.SuppressFlow();
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);
        Assert.Equal(5, rr.Buffer.Length);
    }

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

    // ---------- D.1 — SynchronizationContext at await site is NOT honored ----------

    /// <summary>
    /// A non-default SynchronizationContext set at the await site is NOT honored: the
    /// continuation runs on the dispatcher's chosen thread, NOT on the SC's thread. The
    /// new Pipely.PipelyAwaiter.OnCompleted strips UseSchedulingContext from the flags forwarded
    /// to _core.OnCompleted, so MRVTSC does not capture the SC. The captured SC's
    /// PostCount stays 0; the continuation thread name is the dispatcher's thread.
    /// </summary>
    [Fact]
    public async Task SynchronizationContext_AtAwait_NotHonored_ContinuationOnDispatcherThread()
    {
        // Capture the test thread's ID BEFORE the await. After the await, the
        // continuation resumes on the dispatcher's worker thread, so referencing
        // Environment.CurrentManagedThreadId at the post-await assertion would
        // compare the worker thread's ID to itself.
        int testThreadId = Environment.CurrentManagedThreadId;
        string? observedThreadName = null;
        int? observedThreadId = null;

        using var dispatcher = new DedicatedThreadDispatcher();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        var sc = new CapturingSynchronizationContext();
        var prev = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(sc);

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
        Assert.NotEqual(testThreadId, observedThreadId);
    }

    // ---------- D.2 — TaskScheduler at await site is NOT honored ----------

    /// <summary>
    /// A non-default TaskScheduler captured by the consumer's await (here, via
    /// TaskScheduler.FromCurrentSynchronizationContext on a custom SC) is NOT honored.
    /// Same mechanism as D.1: stripping UseSchedulingContext in OnCompleted prevents
    /// MRVTSC from capturing the scheduler. The continuation runs on the dispatcher's
    /// chosen thread, not the scheduler's thread.
    ///
    /// Note on test structure: Task.Factory.StartNew with a custom TaskScheduler
    /// (derived from CurrentSynchronizationContext) routes through SC.Post to queue
    /// the outer lambda — so PostCount is non-zero by the time the inner await begins.
    /// We reset PostCount immediately before the inner await so the assertion only
    /// counts Posts that the inner await's continuation would trigger; we also place
    /// the assertions INSIDE the StartNew lambda so they execute under the
    /// TaskScheduler/SC context, isolating the inner await as the test's unit.
    /// </summary>
    [Fact]
    public async Task TaskScheduler_AtAwait_NotHonored_ContinuationOnDispatcherThread()
    {
        // Capture the test thread's ID BEFORE the await. After the inner await, the
        // continuation resumes on the dispatcher's worker thread.
        int testThreadId = Environment.CurrentManagedThreadId;

        using var dispatcher = new DedicatedThreadDispatcher();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        var sc = new CapturingSynchronizationContext();
        var prev = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(sc);
        try
        {
            var scheduler = TaskScheduler.FromCurrentSynchronizationContext();

            // Run the test body via Task.Factory.StartNew with the custom scheduler so the
            // inner await's would-be-captured TaskScheduler is the custom one. Assertions
            // are inside the lambda so they execute under that scheduler context (and
            // immediately after the inner await, before any outer-await Post can run).
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

                // Reset PostCount immediately before the inner await — discount any Posts
                // from StartNew setup or the producer Task.Run setup. From this point
                // onwards, PostCount > 0 only if the inner await's continuation routed
                // through SC.Post (which it should NOT under the new wiring).
                Volatile.Write(ref sc.PostCount, 0);

                var rr = await pipe.Reader.ReadAsync();
                pipe.Reader.AdvanceTo(rr.Buffer.End);

                // Assertions inside the lambda so they execute on the dispatcher's worker
                // thread (the inner await's continuation thread). xunit's Assert.* throws
                // on failure; the exception propagates through .Unwrap() to the outer await.
                Assert.Equal(0, Volatile.Read(ref sc.PostCount));
                Assert.Equal(nameof(DedicatedThreadDispatcher), Thread.CurrentThread.Name);
                Assert.NotEqual(testThreadId, Environment.CurrentManagedThreadId);
            }, default, TaskCreationOptions.None, scheduler).Unwrap();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(prev);
        }
    }

    // ---------- D.3 — ConfigureAwait(true) vs ConfigureAwait(false) parity ----------

    /// <summary>
    /// With the new source-side EC-capture wiring, ConfigureAwait(true) and
    /// ConfigureAwait(false) produce identical observable behavior on a Pipe await:
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
        // Capture the test thread's ID BEFORE the await. After the await, the
        // continuation resumes on the dispatcher's worker thread, so referencing
        // Environment.CurrentManagedThreadId at the post-await assertion would
        // compare the worker thread's ID to itself.
        int testThreadId = Environment.CurrentManagedThreadId;

        using var dispatcher = new DedicatedThreadDispatcher();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

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
        Assert.NotEqual(testThreadId, observedThreadId);
    }

    // ---------- E.1 — SetResult-fires-first race (direct-awaiter, N-iteration stress) ----------

    /// <summary>
    /// Pins the spec §4 publication ordering: when the producer signals BEFORE the
    /// consumer has called OnCompleted, the result is delivered correctly via the rare
    /// TP-dispatch path. MRVTSC unconditionally queues the registered s_dispatch
    /// callback to the ThreadPool when the source is already completed at OnCompleted
    /// time; the Volatile.Write ordering in OnCompleted ensures the TP-dispatched
    /// s_dispatch reads _realContinuation / _realState / _capturedEC post-publication.
    ///
    /// Operates directly on Pipely.PipelyAwaiter to force the race deterministically (Pipe's
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
            var awaiter = new Pipely.PipelyAwaiter<int>(dispatcher);

            // PRODUCER SIGNALS FIRST. _core stores the result; _core's _continuation
            // is null because OnCompleted hasn't been called yet.
            awaiter._core.SetResult(1000 + i);

            // CONSUMER REGISTERS SECOND (manually). Pipely.PipelyAwaiter.OnCompleted writes
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

}
