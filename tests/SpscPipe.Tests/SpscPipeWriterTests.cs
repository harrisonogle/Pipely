using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeWriterTests
{
    [Fact]
    public void GetMemory_ReturnsAtLeastSizeHint_AndAdvanceTracksBytes()
    {
        using var pipe = new SpscPipelines.SpscPipe();
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
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        var mem1 = pipe.Writer.GetMemory(64);
        pipe.Writer.Advance(50);

        var mem2 = pipe.Writer.GetMemory(64);   // forces new segment
        Assert.True(mem2.Length >= 64);
    }

    [Fact]
    public void Advance_BeyondCapacity_Throws()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe.Writer.GetMemory(1);
        Assert.Throws<ArgumentOutOfRangeException>(() => pipe.Writer.Advance(int.MaxValue));
    }

    [Fact]
    public void GetMemory_AfterComplete_Throws()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        // Manually set the internal flag to test the entry guard. Complete is wired in Task 9.
        // SpscPipe.Tests has InternalsVisibleTo, so direct field access works.
        pipe._writerCompleted = true;

        Assert.Throws<InvalidOperationException>(() => pipe.Writer.GetMemory(0));
    }

    [Fact]
    public void GetMemory_AfterDispose_Throws()
    {
        var pipe = new SpscPipelines.SpscPipe();
        pipe.Dispose();
        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.GetMemory(0));
    }
}
