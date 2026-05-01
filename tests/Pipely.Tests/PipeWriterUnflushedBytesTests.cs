using Xunit;

namespace PipelyTests;

public class PipeWriterUnflushedBytesTests
{
    [Fact]
    public void CanGetUnflushedBytes_IsTrue()
    {
        using var pipe = new Pipely.Pipe();
        Assert.True(pipe.Writer.CanGetUnflushedBytes);
    }

    [Fact]
    public void UnflushedBytes_ZeroOnFreshPipe()
    {
        using var pipe = new Pipely.Pipe();
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public void UnflushedBytes_ReflectsAdvanceCount()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.UnflushedBytes);

        pipe.Writer.Advance(10);
        Assert.Equal(50L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public async Task UnflushedBytes_ZeroAfterFlushAsync()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.UnflushedBytes);

        await pipe.Writer.FlushAsync();
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public async Task UnflushedBytes_ResumesAfterPostFlushAdvance()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        await pipe.Writer.FlushAsync();
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);

        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(15);
        Assert.Equal(15L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public void UnflushedBytes_ZeroAfterCompleteWithoutFlush()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.UnflushedBytes);

        pipe.Writer.Complete();
        // BCL parity: CompleteWriter's snapshot publish (analogous to BCL's
        // CommitUnsynchronized) drives the count to zero even without a Flush.
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public void UnflushedBytes_ZeroAfterCompleteWithException()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);

        pipe.Writer.Complete(new InvalidOperationException("boom"));
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
    }
}
