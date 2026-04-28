using SpscPipelines.HotHandoff;

namespace SpscPipelines.HotHandoff.Tests;

public class HotHandoffContinuationDispatcherTests
{
    // ---------- Layer A: dispatcher in isolation (no SpscPipe) ----------

    [Fact]
    public void Dispatch_InvokesCallbackOnDedicatedThread()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        int? firstThreadId    = null;
        int? secondThreadId   = null;
        string? firstThreadName  = null;
        string? secondThreadName = null;

        // First dispatch.
        using (var done = new ManualResetEventSlim(false))
        {
            dispatcher.UnsafeQueueUserWorkItem(_ =>
            {
                firstThreadId   = Environment.CurrentManagedThreadId;
                firstThreadName = Thread.CurrentThread.Name;
                done.Set();
            }, null);
            Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
                "First callback was not invoked within 5 seconds.");
        }

        // Second dispatch — must land on the SAME dedicated thread, not a fresh one.
        using (var done = new ManualResetEventSlim(false))
        {
            dispatcher.UnsafeQueueUserWorkItem(_ =>
            {
                secondThreadId   = Environment.CurrentManagedThreadId;
                secondThreadName = Thread.CurrentThread.Name;
                done.Set();
            }, null);
            Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
                "Second callback was not invoked within 5 seconds.");
        }

        Assert.NotEqual(Environment.CurrentManagedThreadId, firstThreadId);
        Assert.Equal("SpscPipe HotHandoff", firstThreadName);
        Assert.Equal("SpscPipe HotHandoff", secondThreadName);
        Assert.Equal(firstThreadId, secondThreadId);   // consistently same dedicated thread
    }

    [Fact]
    public void Dispatch_OverflowFallsBackToThreadPool()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        using var firstStarted = new ManualResetEventSlim(false);
        using var firstRelease = new ManualResetEventSlim(false);
        using var secondDone   = new ManualResetEventSlim(false);
        bool secondOnTpThread = false;

        // First dispatch: claim the slot and hold it until released.
        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            firstStarted.Set();
            firstRelease.Wait(TimeSpan.FromSeconds(5));
        }, null);

        Assert.True(firstStarted.Wait(TimeSpan.FromSeconds(5)),
            "First callback never started — slot was never claimed.");

        // Second dispatch: slot is occupied; should overflow to TP.
        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            secondOnTpThread = Thread.CurrentThread.IsThreadPoolThread;
            secondDone.Set();
        }, null);

        Assert.True(secondDone.Wait(TimeSpan.FromSeconds(5)),
            "Second (overflow) callback was not invoked.");
        Assert.True(secondOnTpThread,
            "Overflow callback should have run on a ThreadPool thread.");

        firstRelease.Set();
    }

    [Fact]
    public void Dispatch_InvokesEachCallbackExactlyOnce()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        const int totalDispatches = 10_000;
        int invocationCount = 0;
        var allDone = new CountdownEvent(totalDispatches);

        Action<object?> cb = _ =>
        {
            Interlocked.Increment(ref invocationCount);
            allDone.Signal();
        };

        // Submit from multiple producer threads to exercise concurrent CAS losers
        // (which fall through to TP).
        Parallel.For(0, totalDispatches, _ => dispatcher.UnsafeQueueUserWorkItem(cb, null));

        Assert.True(allDone.Wait(TimeSpan.FromSeconds(30)),
            $"Not all callbacks ran. Got {invocationCount} of {totalDispatches}.");
        Assert.Equal(totalDispatches, Volatile.Read(ref invocationCount));
    }

    [Fact]
    public void Dispatch_NeverThrowsFromUnsafeQueueUserWorkItem()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        const int totalDispatches = 5_000;
        int dispatchExceptions = 0;

        // Parallel.For is synchronous — when it returns, every iteration body has
        // completed. No CountdownEvent / Wait needed; the signal is implicit.
        Parallel.For(0, totalDispatches, _ =>
        {
            try
            {
                dispatcher.UnsafeQueueUserWorkItem(static _ => { }, null);
            }
            catch
            {
                Interlocked.Increment(ref dispatchExceptions);
            }
        });

        Assert.Equal(0, Volatile.Read(ref dispatchExceptions));
    }

    [Fact]
    public void ThrowingCallback_DoesNotKillDispatcherThread()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        using var firstDone  = new ManualResetEventSlim(false);

        // First slot-path dispatch throws.
        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            firstDone.Set();
            throw new InvalidOperationException("intentional");
        }, null);

        Assert.True(firstDone.Wait(TimeSpan.FromSeconds(5)),
            "First (throwing) callback never ran.");

        // Poll-spin: keep dispatching until one lands on the dedicated thread.
        // Survival is proven by *any* future dispatch being serviced by the
        // worker thread; a TP-fallback observation just means the worker is
        // still mid-cleanup from the throw and we should retry. Bounded by a
        // 5-second deadline (deterministic, no Thread.Sleep).
        string? observedName = null;
        var deadline = Environment.TickCount64 + 5000;
        while (observedName != "SpscPipe HotHandoff" && Environment.TickCount64 < deadline)
        {
            using var probeDone = new ManualResetEventSlim(false);
            dispatcher.UnsafeQueueUserWorkItem(_ =>
            {
                observedName = Thread.CurrentThread.Name;
                probeDone.Set();
            }, null);
            probeDone.Wait(TimeSpan.FromMilliseconds(200));
        }

        Assert.Equal("SpscPipe HotHandoff", observedName);
    }

    [Fact]
    public async Task Dispatch_RacingDispose_InvokesCallbackExactlyOnce()
    {
        // Repeat to flush out the race: the Dispatcher CAS and Dispose's Or both
        // target _state; the spec's Race 1 / Race 2 / Race 4 cases must close every interleaving.
        const int trials = 200;

        for (int trial = 0; trial < trials; trial++)
        {
            var dispatcher = new HotHandoffContinuationDispatcher();
            int invocationCount = 0;
            using var done = new ManualResetEventSlim(false);

            // Two threads racing: one Dispatches, the other Disposes.
            var dispatchTask = Task.Run(() =>
            {
                dispatcher.UnsafeQueueUserWorkItem(_ =>
                {
                    Interlocked.Increment(ref invocationCount);
                    done.Set();
                }, null);
            });
            var disposeTask = Task.Run(() => dispatcher.Dispose());

            await Task.WhenAll(dispatchTask, disposeTask).WaitAsync(TimeSpan.FromSeconds(5));

            Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
                $"Trial {trial}: callback never ran.");
            Assert.Equal(1, Volatile.Read(ref invocationCount));
        }
    }

    [Fact]
    public async Task Dispose_BlocksUntilInFlightCallbackCompletes()
    {
        var dispatcher = new HotHandoffContinuationDispatcher();
        using var callbackStarted = new ManualResetEventSlim(false);
        using var callbackRelease = new ManualResetEventSlim(false);
        int callbackCompleted = 0;

        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            callbackStarted.Set();
            callbackRelease.Wait(TimeSpan.FromSeconds(5));
            Interlocked.Increment(ref callbackCompleted);
        }, null);

        Assert.True(callbackStarted.Wait(TimeSpan.FromSeconds(5)),
            "Callback never started.");

        var disposeTask = Task.Run(() => dispatcher.Dispose());

        // Briefly verify Dispose has not yet returned — the callback is still gated.
        var winner = await Task.WhenAny(disposeTask, Task.Delay(TimeSpan.FromMilliseconds(200)));
        Assert.NotSame(disposeTask, winner);
        Assert.Equal(0, Volatile.Read(ref callbackCompleted));

        callbackRelease.Set();

        await disposeTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, Volatile.Read(ref callbackCompleted));
    }

    [Fact]
    public void Dispatch_AfterDispose_AlwaysRunsOnThreadPool()
    {
        var dispatcher = new HotHandoffContinuationDispatcher();
        dispatcher.Dispose();

        const int total = 100;
        int onTpThread = 0;
        int notOnTpThread = 0;
        var allDone = new CountdownEvent(total);

        for (int i = 0; i < total; i++)
        {
            dispatcher.UnsafeQueueUserWorkItem(_ =>
            {
                if (Thread.CurrentThread.IsThreadPoolThread)
                    Interlocked.Increment(ref onTpThread);
                else
                    Interlocked.Increment(ref notOnTpThread);
                allDone.Signal();
            }, null);
        }

        Assert.True(allDone.Wait(TimeSpan.FromSeconds(10)),
            $"Not all post-Dispose callbacks ran within 10s. onTP={Volatile.Read(ref onTpThread)}, notTP={Volatile.Read(ref notOnTpThread)}");
        Assert.Equal(total, Volatile.Read(ref onTpThread));
        Assert.Equal(0,     Volatile.Read(ref notOnTpThread));
    }

    [Fact]
    public void Dispose_FromWithinCallback_DoesNotDeadlock_AndSubsequentDispatchesRouteToTp()
    {
        // Reproduces the throughput-benchmark hang: a callback running on the
        // worker thread calls dispatcher.Dispose(). Without the
        // Thread.CurrentThread == _thread escape, _thread.Join() self-deadlocks
        // (thread waiting for itself to exit). With the escape, Dispose returns
        // immediately; the worker thread terminates naturally once the cb
        // returns to the loop.
        var dispatcher = new HotHandoffContinuationDispatcher();
        using var firstDone  = new ManualResetEventSlim(false);
        using var secondDone = new ManualResetEventSlim(false);
        bool secondOnTpThread = false;

        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            dispatcher.Dispose();   // T_w Dispose — must not deadlock.
            firstDone.Set();
        }, null);

        Assert.True(firstDone.Wait(TimeSpan.FromSeconds(5)),
            "Self-Dispose deadlocked: the callback never finished.");

        // After Dispose returned, every subsequent Dispatch must route to TP
        // (state has ShutdownRequested set, so Vacant→Busy CAS cannot succeed),
        // even though the worker thread may still be briefly alive while the
        // outer cb finishes returning to the loop.
        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            secondOnTpThread = Thread.CurrentThread.IsThreadPoolThread;
            secondDone.Set();
        }, null);

        Assert.True(secondDone.Wait(TimeSpan.FromSeconds(5)),
            "Post-self-Dispose dispatch never ran.");
        Assert.True(secondOnTpThread,
            "Post-self-Dispose dispatch should route to TP.");
    }

    [Fact]
    public async Task SingleDispatcher_ServingMultiplePipes_CompletesAllAwaiters()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        using var pipeA = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });
        using var pipeB = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

        static async Task Roundtrip(SpscPipelines.SpscPipe pipe, int payloadBytes)
        {
            var readTask = pipe.Reader.ReadAsync().AsTask();
            await Task.Run(async () =>
            {
                var mem = pipe.Writer.GetMemory(payloadBytes);
                mem.Span.Clear();
                pipe.Writer.Advance(payloadBytes);
                await pipe.Writer.FlushAsync();
            });
            var rr = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(payloadBytes, rr.Buffer.Length);
            pipe.Reader.AdvanceTo(rr.Buffer.End);
        }

        // Run both round-trips concurrently — exercises contract item #3
        // (one dispatcher, multiple producer threads).
        await Task.WhenAll(Roundtrip(pipeA, 7), Roundtrip(pipeB, 11));
    }

    // ---------- Layer B: dispatcher integrated with SpscPipe ----------

    [Fact]
    public async Task SpscPipe_WithHotHandoff_BasicReadFlush_RoundTrip()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted, "Reader should park on the empty pipe.");

        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(5);
            mem.Span.Clear();
            pipe.Writer.Advance(5);
            await pipe.Writer.FlushAsync();
        });

        var rr = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(5, rr.Buffer.Length);
        pipe.Reader.AdvanceTo(rr.Buffer.End);
    }

    [Fact]
    public async Task SpscPipe_WithHotHandoff_AsyncLocalFlowsToContinuation()
    {
        var asyncLocal = new AsyncLocal<int>();
        using var dispatcher = new HotHandoffContinuationDispatcher();
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

        asyncLocal.Value = 42;

        // Producer fires after a short delay so the consumer parks first.
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

        // Continuation runs on the dispatcher's worker thread; the captured EC
        // (consumer's, with asyncLocal.Value = 42) is restored by MRVTSC's
        // RunInternal regardless of dispatcher choice.
        Assert.Equal(5, rr.Buffer.Length);
        Assert.Equal(42, asyncLocal.Value);
    }

    [Fact]
    public async Task SpscPipe_WithHotHandoff_DispatcherThreadAsyncLocal_NotObservedInContinuation()
    {
        var consumerLocal   = new AsyncLocal<int>();
        var dispatcherLocal = new AsyncLocal<int>();

        using var dispatcher = new HotHandoffContinuationDispatcher();

        // Set dispatcherLocal on the worker thread by dispatching a one-shot through the slot.
        using var setupDone = new ManualResetEventSlim(false);
        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            dispatcherLocal.Value = 999;
            setupDone.Set();
        }, null);
        Assert.True(setupDone.Wait(TimeSpan.FromSeconds(5)));

        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions { ContinuationDispatcher = dispatcher });

        consumerLocal.Value = 42;

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

        // Continuation runs on the dispatcher's worker thread under the consumer's
        // captured EC. consumerLocal.Value (42) is observed; dispatcherLocal.Value
        // (999, set on the worker thread above) is NOT observed.
        Assert.Equal(5, rr.Buffer.Length);
        Assert.Equal(42, consumerLocal.Value);
        Assert.Equal(0,  dispatcherLocal.Value);
    }

    [Fact]
    public async Task SpscPipe_WithHotHandoff_RapidParkResumeCycles_NoVersionMismatch()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
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
}
