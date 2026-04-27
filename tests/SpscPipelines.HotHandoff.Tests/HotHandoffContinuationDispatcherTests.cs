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
}
