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
}
