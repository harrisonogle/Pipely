using Xunit;

namespace PipelyTests;

public class PipeWriterTests
{
    [Fact]
    public void GetMemory_ReturnsAtLeastSizeHint_AndAdvanceTracksBytes()
    {
        using var pipe = new Pipely.Pipe();
        var mem = pipe.Writer.GetMemory(100);
        Assert.True(mem.Length >= 100);
        for (int i = 0; i < 100; i++) mem.Span[i] = (byte)i;
        pipe.Writer.Advance(100);

        // Subsequent GetMemory returns the next slice.
        var mem2 = pipe.Writer.GetMemory(0);
        Assert.True(mem2.Length >= 1);
    }

    [Fact]
    public void GetMemory_TransitionsToNewSegmentWhenSizeHintExceedsRemaining()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions(minimumSegmentSize: 64));
        var mem1 = pipe.Writer.GetMemory(64);
        pipe.Writer.Advance(50);

        var mem2 = pipe.Writer.GetMemory(64);   // forces new segment
        Assert.True(mem2.Length >= 64);
    }

    [Fact]
    public void Advance_BeyondCapacity_Throws()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(1);
        Assert.Throws<ArgumentOutOfRangeException>(() => pipe.Writer.Advance(int.MaxValue));
    }

    [Fact]
    public void GetMemory_AfterComplete_Throws()
    {
        using var pipe = new Pipely.Pipe();
        // Manually set the internal flag to test the entry guard. Complete is wired in Task 9.
        // Pipely.Tests has InternalsVisibleTo, so direct field access works.
        pipe._writerCompleted = true;

        Assert.Throws<InvalidOperationException>(() => pipe.Writer.GetMemory(0));
    }

    [Fact]
    public void GetMemory_AfterDispose_Throws()
    {
        var pipe = new Pipely.Pipe();
        pipe.Dispose();
        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.GetMemory(0));
    }

    [Fact]
    public async Task FlushAsync_NoBackpressure_ReturnsImmediatelyNotCompleted()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(10);
        pipe.Writer.Advance(10);

        var result = await pipe.Writer.FlushAsync();
        Assert.False(result.IsCanceled);
        Assert.False(result.IsCompleted);
    }

    [Fact]
    public async Task FlushAsync_AfterReaderCompletedNull_ReturnsIsCompletedTrue()
    {
        using var pipe = new Pipely.Pipe();
        // Manually publish a reader-completed state via the readerTb (proxy for Reader.Complete which is Task 9).
        pipe.Writer.GetMemory(10); pipe.Writer.Advance(10);
        var readerSnap = new Pipely.ReaderState { IsCompleted = true, CompletionException = null };
        pipe._readerTb.ProducerSlot() = readerSnap;
        pipe._readerTb.Publish();

        var result = await pipe.Writer.FlushAsync();
        Assert.True(result.IsCompleted);
        Assert.False(result.IsCanceled);
    }

    [Fact]
    public async Task FlushAsync_AfterReaderCompletedException_Throws()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(10); pipe.Writer.Advance(10);
        var ex = new InvalidOperationException("from reader");
        pipe._readerTb.ProducerSlot() = new Pipely.ReaderState { IsCompleted = true, CompletionException = ex };
        pipe._readerTb.Publish();

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Writer.FlushAsync());
        Assert.Same(ex, thrown);
    }

    [Fact]
    public async Task FlushAsync_ParksOnBackpressure_ResumesOnAdvance()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions(
            pauseWriterThreshold: 100,
            resumeWriterThreshold: 50));

        // Fill above pause threshold.
        var mem = pipe.Writer.GetMemory(150);
        pipe.Writer.Advance(150);

        var flushTask = pipe.Writer.FlushAsync().AsTask();
        Assert.False(flushTask.IsCompleted);

        // Reader drains enough to drop below resume threshold.
        await Task.Run(async () =>
        {
            var r = await pipe.Reader.ReadAsync();
            // Consume 110 bytes (leaves 40 unconsumed; below resume threshold of 50).
            pipe.Reader.AdvanceTo(r.Buffer.GetPosition(110));
        });

        var result = await flushTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(result.IsCanceled);
    }
}
