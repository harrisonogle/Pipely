using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeReadInProgressTests
{
    [Fact]
    public async Task ReadAsync_TwiceWithoutAdvanceTo_Throws()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe.Writer.GetMemory(5); pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();

        await pipe.Reader.ReadAsync();

        Assert.Throws<InvalidOperationException>(() => pipe.Reader.ReadAsync());
    }

    [Fact]
    public async Task TryRead_AfterReadAsyncWithoutAdvanceTo_Throws()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe.Writer.GetMemory(5); pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();

        await pipe.Reader.ReadAsync();

        Assert.Throws<InvalidOperationException>(() => pipe.Reader.TryRead(out _));
    }

    [Fact]
    public async Task ReadAsync_AfterReadAsyncAndAdvanceTo_Works()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe.Writer.GetMemory(5); pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();

        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.End);

        pipe.Writer.GetMemory(3); pipe.Writer.Advance(3);
        await pipe.Writer.FlushAsync();

        var r2 = await pipe.Reader.ReadAsync();
        Assert.Equal(3, r2.Buffer.Length);
    }

    [Fact]
    public void TryRead_ReturnsFalse_DoesNotMarkInProgress()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        Assert.False(pipe.Reader.TryRead(out _));
        Assert.False(pipe.Reader.TryRead(out _));   // would throw if first call had set the flag
    }

    [Fact]
    public async Task TryRead_ReturnsTrue_MarksInProgress()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe.Writer.GetMemory(5); pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();

        Assert.True(pipe.Reader.TryRead(out _));
        Assert.Throws<InvalidOperationException>(() => pipe.Reader.TryRead(out _));
    }

    [Fact]
    public async Task ReadAsync_TokenCancelsWhileParked_NextReadAsyncWorks()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var cts = new CancellationTokenSource();
        var t = pipe.Reader.ReadAsync(cts.Token).AsTask();
        Assert.False(t.IsCompleted);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await t);

        // SetException path must not leave _readPending stuck — next ReadAsync should park, not throw.
        var t2 = pipe.Reader.ReadAsync().AsTask();
        Assert.False(t2.IsCompleted);

        // Cleanup so the test exits cleanly.
        pipe.Writer.Complete();
        await t2.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ReadAsync_WriterCompleteWithExWhileParked_NextReadAsyncStillThrowsViaEntryGuard()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var t = pipe.Reader.ReadAsync().AsTask();
        Assert.False(t.IsCompleted);

        var ex = new InvalidOperationException("from writer");
        pipe.Writer.Complete(ex);

        var thrown1 = await Assert.ThrowsAsync<InvalidOperationException>(async () => await t);
        Assert.Same(ex, thrown1);

        // _readPending was never set on the SetException path. The next ReadAsync should
        // throw the writer-completion-exception via the existing entry guard (R7), NOT
        // "Reading is in progress."
        var thrown2 = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Reader.ReadAsync());
        Assert.Same(ex, thrown2);
    }

    [Fact]
    public async Task CancelPendingRead_DeliversCanceled_NextReadRequiresAdvanceTo()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var t = pipe.Reader.ReadAsync().AsTask();
        Assert.False(t.IsCompleted);

        await Task.Run(() => pipe.Reader.CancelPendingRead());
        var r = await t.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(r.IsCanceled);

        // CancelPendingRead delivered a ReadResult — even with empty buffer, AdvanceTo is required.
        Assert.Throws<InvalidOperationException>(() => pipe.Reader.ReadAsync());
    }

    [Fact]
    public async Task ReadAsync_StickyCancelSyncReturn_RequiresAdvanceTo()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe.Reader.CancelPendingRead();   // sets sticky cancel; no parked awaiter

        var r = await pipe.Reader.ReadAsync();
        Assert.True(r.IsCanceled);

        // Sticky-cancel sync return is still a ReadResult delivery — needs AdvanceTo.
        Assert.Throws<InvalidOperationException>(() => pipe.Reader.ReadAsync());
    }
}
