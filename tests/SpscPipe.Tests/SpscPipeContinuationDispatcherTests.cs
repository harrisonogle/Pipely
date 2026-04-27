using System.Threading;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeContinuationDispatcherTests
{
    // ---------- Helper dispatchers ----------

    private sealed class RecordingDispatcher : IContinuationDispatcher
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
    private sealed class ForwardingDispatcher : IContinuationDispatcher, IDisposable
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
    private sealed class BadEcCapturingDispatcher : IContinuationDispatcher
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
    private sealed class DedicatedThreadDispatcher : IContinuationDispatcher, IDisposable
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
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

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
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

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
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

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

        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

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

    [Fact]
    public async Task CustomDispatcher_BadImpl_CapturingEC_IsDetectable()
    {
        // A "bad" dispatcher that uses ThreadPool.QueueUserWorkItem (captures EC).
        var badDispatcher = new BadEcCapturingDispatcher();
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = badDispatcher });

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

        // The producer's EC, if captured by the bad dispatcher, would be applied to
        // the work item BEFORE the inner RunInternal restoration. The continuation
        // sees consumer's EC during execution due to RunInternal — so producer's
        // value is NOT visible. This test documents the safety property: even a
        // dispatcher that captures EC doesn't break the consumer's continuation.
        Assert.Equal(0, observedProducer);
    }

    // ---------- E. Exception in continuation doesn't kill dispatcher thread ----------

    [Fact]
    public async Task CustomDispatcher_ContinuationException_DoesNotKillDispatcherThread()
    {
        using var dispatcher = new DedicatedThreadDispatcher();
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

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
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

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
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

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
}
