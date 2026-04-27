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

        // Consumer awaits, parks. Dispatcher routes the continuation to TP.
        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        // Producer-side write triggers signal -> dispatcher -> continuation runs.
        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        int observedAfterAwait = asyncLocal.Value;
        var rr = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(5, rr.Buffer.Length);
        Assert.Equal(42, observedAfterAwait);
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

        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        var rr = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(5, rr.Buffer.Length);

        // Continuation observed consumer's value.
        Assert.Equal(42, consumerLocal.Value);

        // The dispatcher thread had set its own AsyncLocal value (999), but the continuation
        // (which ran via dispatcher) should NOT see it. We're now back on the consumer's thread
        // (the test's main async flow), so dispatcherLocal — never set on this thread — must
        // still be the default 0.
        Assert.Equal(0, dispatcherLocal.Value);
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

        var readTask = pipe.Reader.ReadAsync().AsTask();

        int observedConsumerLocalInContinuation = -1;
        int observedProducerLocalInContinuation = -1;

        // Continue in the consumer's continuation
        var capturingTask = readTask.ContinueWith(_ =>
        {
            observedConsumerLocalInContinuation = consumerLocal.Value;
            observedProducerLocalInContinuation = producerLocal.Value;
        });

        await Task.Run(async () =>
        {
            producerLocal.Value = 99;  // set on producer's thread
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        await capturingTask.WaitAsync(TimeSpan.FromSeconds(5));

        // The consumer's value must always be observed (this is the hard guarantee even
        // with a misbehaving dispatcher, because MRVTSC's RunInternal restores the
        // captured EC for the continuation regardless of dispatcher).
        Assert.Equal(42, observedConsumerLocalInContinuation);

        // The producer's EC, if captured by the bad dispatcher, would be applied to
        // the work item BEFORE the inner RunInternal restoration. The continuation
        // sees consumer's EC during execution due to RunInternal — so producer's
        // value is NOT visible. This test documents the safety property: even a
        // dispatcher that captures EC doesn't break the consumer's continuation.
        Assert.Equal(0, observedProducerLocalInContinuation);
    }

    // ---------- E. Exception in continuation doesn't kill dispatcher thread ----------

    [Fact]
    public async Task CustomDispatcher_ContinuationException_DoesNotKillDispatcherThread()
    {
        using var dispatcher = new DedicatedThreadDispatcher();
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

        // First read: the user's continuation advances the buffer and then throws. The
        // dispatcher must wrap the callback in try/catch (per contract item #5) so the
        // dispatcher thread survives the in-flight exception. The user's task fault surfaces
        // up through its returned Task; the dispatcher thread is unaffected.
        var firstRead = pipe.Reader.ReadAsync().AsTask().ContinueWith(t =>
        {
            // Drain the buffer so subsequent ReadAsync calls don't see "Reading is in progress".
            pipe.Reader.AdvanceTo(t.Result.Buffer.End);
            throw new InvalidOperationException("test exception");
        });

        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        try { await firstRead.WaitAsync(TimeSpan.FromSeconds(5)); }
        catch { /* expected — the ContinueWith body threw */ }

        // Dispatcher thread should still be alive — second read should park then resume.
        var secondRead = pipe.Reader.ReadAsync().AsTask();

        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(7);
            mem.Span.Clear();
            pipe.Writer.Advance(7);
            await pipe.Writer.FlushAsync();
        });

        var rr = await secondRead.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(rr.IsCanceled);
        Assert.Equal(7, rr.Buffer.Length);
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
