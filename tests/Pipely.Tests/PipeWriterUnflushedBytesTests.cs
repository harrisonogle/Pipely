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
}
