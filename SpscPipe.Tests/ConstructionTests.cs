using SpscPipe;

namespace SpscPipe.Tests;

// Checkpoint 1 coverage: validate construction and options per §3.
// Behavioral tests (round-trip, backpressure, etc.) arrive in later checkpoints.
public class ConstructionTests
{
    [Fact]
    public void DefaultConstructor_Succeeds_And_ExposesReaderAndWriter()
    {
        var pipe = new global::SpscPipe.SpscPipe();
        Assert.NotNull(pipe.Reader);
        Assert.NotNull(pipe.Writer);
    }

    [Fact]
    public void OptionsConstructor_WithValidOptions_Succeeds()
    {
        var options = new SpscPipeOptions
        {
            MinimumSegmentSize    = 4096,
            PauseWriterThreshold  = 65536,
            ResumeWriterThreshold = 32768,
        };

        var pipe = new global::SpscPipe.SpscPipe(options);
        Assert.NotNull(pipe.Reader);
        Assert.NotNull(pipe.Writer);
    }

    [Fact]
    public void OptionsConstructor_WithNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new global::SpscPipe.SpscPipe(null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void OptionsConstructor_WithMinimumSegmentSizeLessThanOne_Throws(int size)
    {
        var options = new SpscPipeOptions { MinimumSegmentSize = size };
        Assert.Throws<ArgumentOutOfRangeException>(() => new global::SpscPipe.SpscPipe(options));
    }

    [Fact]
    public void OptionsConstructor_WithNegativePauseThreshold_Throws()
    {
        var options = new SpscPipeOptions { PauseWriterThreshold = -1 };
        Assert.Throws<ArgumentOutOfRangeException>(() => new global::SpscPipe.SpscPipe(options));
    }

    [Fact]
    public void OptionsConstructor_WithNegativeResumeThreshold_Throws()
    {
        var options = new SpscPipeOptions { ResumeWriterThreshold = -1 };
        Assert.Throws<ArgumentOutOfRangeException>(() => new global::SpscPipe.SpscPipe(options));
    }

    [Fact]
    public void OptionsConstructor_WithResumeAbovePause_Throws()
    {
        var options = new SpscPipeOptions
        {
            PauseWriterThreshold  = 1000,
            ResumeWriterThreshold = 2000,
        };
        Assert.Throws<ArgumentOutOfRangeException>(() => new global::SpscPipe.SpscPipe(options));
    }

    [Fact]
    public void OptionsConstructor_WithEqualThresholds_Succeeds()
    {
        // The invariant is Resume <= Pause, so equality is permitted.
        var options = new SpscPipeOptions
        {
            PauseWriterThreshold  = 1000,
            ResumeWriterThreshold = 1000,
        };
        var pipe = new global::SpscPipe.SpscPipe(options);
        Assert.NotNull(pipe.Reader);
    }
}
