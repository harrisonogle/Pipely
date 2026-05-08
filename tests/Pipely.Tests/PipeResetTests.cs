using Xunit;

namespace PipelyTests;

public class PipeResetTests
{
    [Fact]
    public void Reset_NeitherSideCompleted_Throws()
    {
        var pipe = new Pipely.Pipe();
        var ex = Assert.Throws<InvalidOperationException>(() => pipe.Reset());
        Assert.Equal("Both completion routines must be called before resetting the pipe.", ex.Message);
    }

    [Fact]
    public void Reset_OnlyWriterCompleted_Throws()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete();
        Assert.Throws<InvalidOperationException>(() => pipe.Reset());
    }

    [Fact]
    public void Reset_OnlyReaderCompleted_Throws()
    {
        var pipe = new Pipely.Pipe();
        pipe.Reader.Complete();
        Assert.Throws<InvalidOperationException>(() => pipe.Reset());
    }

    [Fact]
    public void Reset_BothCompleted_DoesNotThrow()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();   // must not throw
    }
}
