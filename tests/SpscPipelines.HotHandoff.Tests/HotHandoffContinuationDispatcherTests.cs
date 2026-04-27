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
}
