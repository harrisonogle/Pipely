using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeLifecycleTests
{
    [Fact]
    public async Task WriterCompleteNull_ReaderSeesIsCompletedAfterDrain()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var mem = pipe.Writer.GetMemory(5); mem.Span.Fill(0xAA); pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();
        pipe.Writer.Complete();

        var r1 = await pipe.Reader.ReadAsync();
        Assert.True(r1.IsCompleted);
        Assert.Equal(5, r1.Buffer.Length);

        pipe.Reader.AdvanceTo(r1.Buffer.End);

        var r2 = await pipe.Reader.ReadAsync();
        Assert.True(r2.IsCompleted);
        Assert.True(r2.Buffer.IsEmpty);
    }

    [Fact]
    public async Task WriterCompleteEx_EveryReadAsyncThrows()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var ex = new InvalidOperationException("writer error");
        pipe.Writer.Complete(ex);

        var t1 = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Reader.ReadAsync());
        Assert.Same(ex, t1);

        var t2 = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Reader.ReadAsync());
        Assert.Same(ex, t2);
    }

    [Fact]
    public async Task ReaderComplete_WriterFlushReturnsIsCompleted()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe.Reader.Complete();

        var r = await pipe.Writer.FlushAsync();
        Assert.True(r.IsCompleted);
    }

    [Fact]
    public async Task ReaderCompleteEx_WriterFlushThrows()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var ex = new InvalidOperationException("reader error");
        pipe.Reader.Complete(ex);

        var t = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Writer.FlushAsync());
        Assert.Same(ex, t);
    }

    [Fact]
    public void DoubleComplete_NoOp()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe.Writer.Complete();
        pipe.Writer.Complete(new Exception("ignored"));         // no-op coalesce
        pipe.Reader.Complete();
        pipe.Reader.Complete(new Exception("ignored"));
    }

    [Fact]
    public async Task ReaderComplete_AllChainSegmentsRecycledOnNextFlush()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        // Fill two segments.
        for (int i = 0; i < 2; i++)
        {
            pipe.Writer.GetMemory(60); pipe.Writer.Advance(60);
            await pipe.Writer.FlushAsync();
        }
        pipe.Reader.Complete();

        // Writer's next FlushAsync should recycle the entire chain (HeadSegment=null + IsCompleted=true).
        await pipe.Writer.FlushAsync();

        // _chainHead should equal _writingHead (only the active tail remains).
        Assert.Same(pipe._writingHead, pipe._chainHead);
    }
}
