using SpscPipelines.HotHandoff;

namespace SpscPipelines.HotHandoff.Tests;

public class HotHandoffContinuationDispatcherTests
{
    // Tests added in subsequent tasks.

    [Fact]
    public void Dispatch_InvokesCallbackOnDedicatedThread()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        int? observedThreadId = null;
        string? observedThreadName = null;
        using var done = new ManualResetEventSlim(false);

        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            observedThreadId = Environment.CurrentManagedThreadId;
            observedThreadName = Thread.CurrentThread.Name;
            done.Set();
        }, null);

        Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
            "Callback was not invoked within 5 seconds.");
        Assert.NotEqual(Environment.CurrentManagedThreadId, observedThreadId);
        Assert.Equal("SpscPipe HotHandoff", observedThreadName);
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
        var allDispatched = new CountdownEvent(totalDispatches);

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
            finally
            {
                allDispatched.Signal();
            }
        });

        Assert.True(allDispatched.Wait(TimeSpan.FromSeconds(30)));
        Assert.Equal(0, Volatile.Read(ref dispatchExceptions));
    }

    [Fact]
    public void ThrowingCallback_DoesNotKillDispatcherThread()
    {
        using var dispatcher = new HotHandoffContinuationDispatcher();
        using var firstDone  = new ManualResetEventSlim(false);
        using var secondDone = new ManualResetEventSlim(false);
        string? secondThreadName = null;

        // First slot-path dispatch throws.
        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            firstDone.Set();
            throw new InvalidOperationException("intentional");
        }, null);

        Assert.True(firstDone.Wait(TimeSpan.FromSeconds(5)),
            "First (throwing) callback never ran.");

        // Give the dispatcher thread a moment to finish processing the throw + re-loop.
        Thread.Sleep(50);

        // Second slot-path dispatch must run on the same dedicated thread —
        // the worker survived the throw.
        dispatcher.UnsafeQueueUserWorkItem(_ =>
        {
            secondThreadName = Thread.CurrentThread.Name;
            secondDone.Set();
        }, null);

        Assert.True(secondDone.Wait(TimeSpan.FromSeconds(5)),
            "Second callback after throwing first never ran — dispatcher thread may have died.");
        Assert.Equal("SpscPipe HotHandoff", secondThreadName);
    }

    [Fact]
    public void Dispatch_RacingDispose_InvokesCallbackExactlyOnce()
    {
        // Repeat to flush out the race: the Dispatcher CAS and Dispose's Or both
        // target _state; the spec's Race 1 / Race 4 cases must close every interleaving.
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

            Task.WaitAll(new[] { dispatchTask, disposeTask }, TimeSpan.FromSeconds(5));

            Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
                $"Trial {trial}: callback never ran.");
            Assert.Equal(1, Volatile.Read(ref invocationCount));
        }
    }
}
