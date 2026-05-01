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
}
