using System.Buffers;
using Xunit;

namespace PipelyTests;

public class PipeReaderTests
{
    [Fact]
    public async Task ReadAsync_AfterFlushedData_ReturnsBuffer()
    {
        var pipe = new Pipely.Pipe();
        var mem = pipe.Writer.GetMemory(5);
        mem.Span[0] = 1; mem.Span[1] = 2; mem.Span[2] = 3; mem.Span[3] = 4; mem.Span[4] = 5;
        pipe.Writer.Advance(5);
        await pipe.Writer.FlushAsync();

        var result = await pipe.Reader.ReadAsync();
        Assert.False(result.IsCanceled);
        Assert.False(result.IsCompleted);
        Assert.Equal(5, result.Buffer.Length);
        var arr = result.Buffer.ToArray();
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, arr);
    }

    [Fact]
    public void TryRead_NoData_ReturnsFalse()
    {
        var pipe = new Pipely.Pipe();
        Assert.False(pipe.Reader.TryRead(out var result));
    }

    [Fact]
    public async Task ReadAsync_AfterWriterCompletedNull_ReturnsIsCompletedTrue()
    {
        var pipe = new Pipely.Pipe();
        // Simulate Writer.Complete(null) by direct WriterState publish (real Complete in Task 9).
        pipe._writerTb.ProducerSlot() = new Pipely.WriterState { IsCompleted = true };
        pipe._writerTb.Publish();

        var result = await pipe.Reader.ReadAsync();
        Assert.True(result.IsCompleted);
        Assert.True(result.Buffer.IsEmpty);
    }

    [Fact]
    public async Task ReadAsync_AfterWriterCompletedWithEx_Throws()
    {
        var pipe = new Pipely.Pipe();
        var ex = new InvalidOperationException("from writer");
        pipe._writerTb.ProducerSlot() = new Pipely.WriterState { IsCompleted = true, CompletionException = ex };
        pipe._writerTb.Publish();

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Reader.ReadAsync());
        Assert.Same(ex, thrown);
    }

    [Fact]
    public async Task ReadAsync_ParksWhenNoData_ResumesOnFlush()
    {
        var pipe = new Pipely.Pipe();
        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        // Writer side on a different thread.
        await Task.Run(async () =>
        {
            var mem = pipe.Writer.GetMemory(3);
            mem.Span[0] = 7; mem.Span[1] = 8; mem.Span[2] = 9;
            pipe.Writer.Advance(3);
            await pipe.Writer.FlushAsync();
        });

        var result = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new byte[] { 7, 8, 9 }, result.Buffer.ToArray());
    }
}
