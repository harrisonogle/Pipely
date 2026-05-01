using Xunit;

namespace PipelyTests;

public class PipeWriterBufferedBytesTests
{
    [Fact]
    public void BufferedBytes_ZeroOnFreshPipe()
    {
        using var pipe = new Pipely.Pipe();
        Assert.Equal(0L, pipe.Writer.BufferedBytes);
    }

    [Fact]
    public void BufferedBytes_ReflectsAdvanceCount_BeforeFlush()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.BufferedBytes);

        pipe.Writer.Advance(10);
        Assert.Equal(50L, pipe.Writer.BufferedBytes);
    }

    [Fact]
    public async Task BufferedBytes_RemainsAfterFlush_WhenReaderHasNotConsumed()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.BufferedBytes);

        await pipe.Writer.FlushAsync();
        // Unlike UnflushedBytes, the bytes are still buffered until the reader consumes.
        Assert.Equal(0L, pipe.Writer.UnflushedBytes);
        Assert.Equal(40L, pipe.Writer.BufferedBytes);
    }

    [Fact]
    public async Task BufferedBytes_DropsAfterReaderConsumes_AndWriterFlushes()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        await pipe.Writer.FlushAsync();
        Assert.Equal(40L, pipe.Writer.BufferedBytes);

        var result = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(result.Buffer.End);

        // Stale until the writer next acquires reader state.
        Assert.Equal(40L, pipe.Writer.BufferedBytes);

        // FlushAsync refreshes _lastAcquiredReaderState; published TotalConsumed is now 40.
        await pipe.Writer.FlushAsync();
        Assert.Equal(0L, pipe.Writer.BufferedBytes);
    }

    [Fact]
    public async Task BufferedBytes_StaleUntilNextFlush_BoundedByFlushCadence()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        await pipe.Writer.FlushAsync();

        var result = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(result.Buffer.End);

        // No flush since the consume — writer's view is stale by design.
        Assert.Equal(40L, pipe.Writer.BufferedBytes);

        // Successive reads/advances change nothing on the writer side without a flush.
        Assert.Equal(40L, pipe.Writer.BufferedBytes);
    }

    [Fact]
    public void BufferedBytes_NonZeroAfterCompleteWithoutFlush_WhenReaderHasNotConsumed()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.BufferedBytes);

        // Complete publishes a writer snapshot but does not refresh _lastAcquiredReaderState,
        // and the reader has consumed nothing — bytes are still buffered.
        pipe.Writer.Complete();
        Assert.Equal(40L, pipe.Writer.BufferedBytes);
    }

    [Fact]
    public void BufferedBytes_ReflectsSpliceLength_OnEmptyPipe()
    {
        using var pipe = new Pipely.Pipe();
        var owner = new TrackingMemoryOwner(64);

        pipe.Writer.Splice(owner);
        Assert.Equal(64L, pipe.Writer.BufferedBytes);
    }

    [Fact]
    public void BufferedBytes_AccumulatesAcrossAdvanceAndSplice()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(20);
        Assert.Equal(20L, pipe.Writer.BufferedBytes);

        var owner = new TrackingMemoryOwner(64);
        pipe.Writer.Splice(owner);
        Assert.Equal(20L + 64L, pipe.Writer.BufferedBytes);

        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(5);
        Assert.Equal(20L + 64L + 5L, pipe.Writer.BufferedBytes);
    }

    [Fact]
    public async Task BufferedBytes_SupersetOfUnflushedBytes()
    {
        using var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        await pipe.Writer.FlushAsync();

        // Reader has not consumed: published bytes still buffered.
        // UnflushedBytes only counts the staging side.
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(20);

        Assert.Equal(20L, pipe.Writer.UnflushedBytes);
        Assert.Equal(60L, pipe.Writer.BufferedBytes);
        Assert.True(pipe.Writer.BufferedBytes >= pipe.Writer.UnflushedBytes);
    }

    [Fact]
    public void BufferedBytes_AfterDispose_ReturnsLastValue_NoThrow()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.GetMemory(100);
        pipe.Writer.Advance(40);
        Assert.Equal(40L, pipe.Writer.BufferedBytes);

        pipe.Dispose();

        // Pure accessor, no validation. Dispose does not touch _totalWritten or
        // _lastAcquiredReaderState, so the last value is still observable.
        Assert.Equal(40L, pipe.Writer.BufferedBytes);
    }
}
