using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.InteropServices;

namespace PipelyTests;

public class FastSchedulerTests
{
    // ---------- Layer A: dispatcher in isolation (no Pipe) ----------

    [Fact]
    public void Dispatch_InvokesCallbackOnDedicatedThread()
    {
        using var dispatcher = new Pipely.FastScheduler();
        int? firstThreadId    = null;
        int? secondThreadId   = null;
        string? firstThreadName  = null;
        string? secondThreadName = null;

        // First dispatch.
        using (var done = new ManualResetEventSlim(false))
        {
            dispatcher.Schedule(_ =>
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
            dispatcher.Schedule(_ =>
            {
                secondThreadId   = Environment.CurrentManagedThreadId;
                secondThreadName = Thread.CurrentThread.Name;
                done.Set();
            }, null);
            Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
                "Second callback was not invoked within 5 seconds.");
        }

        Assert.NotEqual(Environment.CurrentManagedThreadId, firstThreadId);
        Assert.Equal("Pipe FastScheduler", firstThreadName);
        Assert.Equal("Pipe FastScheduler", secondThreadName);
        Assert.Equal(firstThreadId, secondThreadId);   // consistently same dedicated thread
    }

    [Fact]
    public void Dispatch_OverflowFallsBackToThreadPool()
    {
        using var dispatcher = new Pipely.FastScheduler();
        using var firstStarted = new ManualResetEventSlim(false);
        using var firstRelease = new ManualResetEventSlim(false);
        using var secondDone   = new ManualResetEventSlim(false);
        bool secondOnTpThread = false;

        // First dispatch: claim the slot and hold it until released.
        dispatcher.Schedule(_ =>
        {
            firstStarted.Set();
            firstRelease.Wait(TimeSpan.FromSeconds(5));
        }, null);

        Assert.True(firstStarted.Wait(TimeSpan.FromSeconds(5)),
            "First callback never started — slot was never claimed.");

        // Second dispatch: slot is occupied; should overflow to TP.
        dispatcher.Schedule(_ =>
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
        using var dispatcher = new Pipely.FastScheduler();
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
        Parallel.For(0, totalDispatches, _ => dispatcher.Schedule(cb, null));

        Assert.True(allDone.Wait(TimeSpan.FromSeconds(30)),
            $"Not all callbacks ran. Got {invocationCount} of {totalDispatches}.");
        Assert.Equal(totalDispatches, Volatile.Read(ref invocationCount));
    }

    [Fact]
    public void Dispatch_NeverThrowsFromSchedule()
    {
        using var dispatcher = new Pipely.FastScheduler();
        const int totalDispatches = 5_000;
        int dispatchExceptions = 0;

        // Parallel.For is synchronous — when it returns, every iteration body has
        // completed. No CountdownEvent / Wait needed; the signal is implicit.
        Parallel.For(0, totalDispatches, _ =>
        {
            try
            {
                dispatcher.Schedule(static _ => { }, null);
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
        using var dispatcher = new Pipely.FastScheduler();
        using var firstDone  = new ManualResetEventSlim(false);

        // First slot-path dispatch throws.
        dispatcher.Schedule(_ =>
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
        while (observedName != "Pipe FastScheduler" && Environment.TickCount64 < deadline)
        {
            using var probeDone = new ManualResetEventSlim(false);
            dispatcher.Schedule(_ =>
            {
                observedName = Thread.CurrentThread.Name;
                probeDone.Set();
            }, null);
            probeDone.Wait(TimeSpan.FromMilliseconds(200));
        }

        Assert.Equal("Pipe FastScheduler", observedName);
    }

    [Fact]
    public async Task Dispatch_RacingDispose_InvokesCallbackExactlyOnce()
    {
        // Repeat to flush out the race: the Dispatcher CAS and Dispose's Or both
        // target _state; the spec's Race 1 / Race 2 / Race 4 cases must close every interleaving.
        const int trials = 200;

        for (int trial = 0; trial < trials; trial++)
        {
            var dispatcher = new Pipely.FastScheduler();
            int invocationCount = 0;
            using var done = new ManualResetEventSlim(false);

            // Two threads racing: one Dispatches, the other Disposes.
            var dispatchTask = Task.Run(() =>
            {
                dispatcher.Schedule(_ =>
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
        var dispatcher = new Pipely.FastScheduler();
        using var callbackStarted = new ManualResetEventSlim(false);
        using var callbackRelease = new ManualResetEventSlim(false);
        int callbackCompleted = 0;

        dispatcher.Schedule(_ =>
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
        var dispatcher = new Pipely.FastScheduler();
        dispatcher.Dispose();

        const int total = 100;
        int onTpThread = 0;
        int notOnTpThread = 0;
        var allDone = new CountdownEvent(total);

        for (int i = 0; i < total; i++)
        {
            dispatcher.Schedule(_ =>
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
        var dispatcher = new Pipely.FastScheduler();
        using var firstDone  = new ManualResetEventSlim(false);
        using var secondDone = new ManualResetEventSlim(false);
        bool secondOnTpThread = false;

        dispatcher.Schedule(_ =>
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
        dispatcher.Schedule(_ =>
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
        using var dispatcher = new Pipely.FastScheduler();
        using var pipeA = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });
        using var pipeB = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

        static async Task Roundtrip(Pipely.Pipe pipe, int payloadBytes)
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

    // ---------- Layer B: dispatcher integrated with Pipe ----------

    [Fact]
    public async Task Pipe_WithFastScheduler_BasicReadFlush_RoundTrip()
    {
        using var dispatcher = new Pipely.FastScheduler();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

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
    public async Task Pipe_WithFastScheduler_AsyncLocalFlowsToContinuation()
    {
        var asyncLocal = new AsyncLocal<int>();
        using var dispatcher = new Pipely.FastScheduler();
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

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
    public async Task Pipe_WithFastScheduler_DispatcherThreadAsyncLocal_NotObservedInContinuation()
    {
        var consumerLocal   = new AsyncLocal<int>();
        var dispatcherLocal = new AsyncLocal<int>();

        using var dispatcher = new Pipely.FastScheduler();

        // Set dispatcherLocal on the worker thread by dispatching a one-shot through the slot.
        using var setupDone = new ManualResetEventSlim(false);
        dispatcher.Schedule(_ =>
        {
            dispatcherLocal.Value = 999;
            setupDone.Set();
        }, null);
        Assert.True(setupDone.Wait(TimeSpan.FromSeconds(5)));

        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions { ContinuationDispatcher = dispatcher });

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
    public async Task Pipe_WithFastScheduler_RapidParkResumeCycles_NoVersionMismatch()
    {
        using var dispatcher = new Pipely.FastScheduler();
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

    // ---------- Layer C: BDN-pattern stress repro (diagnostic) ----------

    /// <summary>
    /// Reproduces the BDN deadlock pattern outside of BDN to determine whether
    /// it's a FastScheduler bug or BDN-harness-specific.
    ///
    /// Background: when the BDN throughput benchmark's consumer was modified
    /// to do per-message timestamp processing (CopyTo + MemoryMarshal.Read +
    /// Stopwatch.GetTimestamp + sample-write per chunk, mirroring the latency
    /// CLI's pattern), BDN's WorkloadJitting hung deterministically on the
    /// FastScheduler benchmark — twice in a row, on commits 0d10225 and 032528f.
    /// Reverting to a simple-drain consumer (commit 99293c1) cleared it. The
    /// latency CLI runs the same per-message consumer code happily, so the
    /// hypothesis was that the combination of (sustained back-to-back
    /// iterations sharing one dispatcher) + (per-message consumer that holds
    /// the slot longer via inline continuation processing) surfaces a
    /// FastScheduler bug that the latency CLI's slower iteration cadence doesn't.
    ///
    /// This test reproduces that exact pattern outside of BDN, with all
    /// per-iteration state local (no shared <c>_samples</c> field) so we are
    /// testing the dispatcher's behavior under sustained-iteration pressure
    /// alone, not any cross-iteration state interactions.
    ///
    /// Outcome interpretation:
    /// <list type="bullet">
    /// <item>If this test passes within the timeout, the BDN deadlock is in
    ///       BDN's harness layer (or in the cross-iteration state we ruled
    ///       out here), not in FastScheduler itself.</item>
    /// <item>If this test hangs, FastScheduler has a real bug under this pattern;
    ///       we have a reproducer to investigate further.</item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task Pipe_WithFastScheduler_RepeatedIteration_PerMessageConsumer_DoesNotHang()
    {
        using var dispatcher = new Pipely.FastScheduler();
        const int iterations   = 30;            // BDN's WorkloadJitting hung at op 16; 30 gives margin.
        const int messageCount = 1_000_000;     // Same as the BDN temp workload (commit c1ba6b9).
        const int chunkSize    = 256;           // Same as the BDN temp workload.

        var driver = Task.Run(async () =>
        {
            for (int iter = 0; iter < iterations; iter++)
            {
                using var pipe = new Pipely.Pipe(new Pipely.PipeOptions
                {
                    ContinuationDispatcher = dispatcher,
                });
                await ProduceAndDrainPerMessage(pipe.Reader, pipe.Writer, messageCount, chunkSize);
            }
        });

        // Bounded timeout: the workload should complete in ~10-30 seconds in
        // normal conditions. If the dispatcher hangs under this pattern,
        // WaitAsync throws TimeoutException rather than blocking the test
        // runner indefinitely.
        await driver.WaitAsync(TimeSpan.FromMinutes(2));
    }

    private static async Task ProduceAndDrainPerMessage(
        PipeReader reader, PipeWriter writer, int messageCount, int chunkSize)
    {
        long bytesTotal = (long)messageCount * chunkSize;
        // Local samples array — fresh per iteration. Rules out cross-iteration
        // state interactions as a confounder.
        var samples = new long[messageCount];

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < messageCount; i++)
            {
                var memory = writer.GetMemory(chunkSize);
                long t = Stopwatch.GetTimestamp();
                MemoryMarshal.Write(memory.Span, in t);
                writer.Advance(chunkSize);
                await writer.FlushAsync();
            }
            writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            long consumed = 0;
            int messageIdx = 0;
            byte[] tsBuf = new byte[8];
            while (consumed < bytesTotal)
            {
                var rr = await reader.ReadAsync();
                var buf = rr.Buffer;
                while (buf.Length >= chunkSize)
                {
                    buf.Slice(0, 8).CopyTo(tsBuf);
                    long sentTicks = MemoryMarshal.Read<long>(tsBuf);
                    long now = Stopwatch.GetTimestamp();
                    samples[messageIdx++] = now - sentTicks;
                    consumed += chunkSize;
                    buf = buf.Slice(chunkSize);
                }
                long consumedThisRead = rr.Buffer.Length - buf.Length;
                reader.AdvanceTo(rr.Buffer.GetPosition(consumedThisRead), rr.Buffer.End);
                if (rr.IsCompleted && consumed >= bytesTotal) break;
            }
            reader.Complete();
        });

        await Task.WhenAll(producer, consumer);
    }
}
