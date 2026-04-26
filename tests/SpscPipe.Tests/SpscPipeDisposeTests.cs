using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeDisposeTests
{
    [Fact]
    public void Dispose_NeverUsed_NoThrow()
    {
        var pipe = new SpscPipelines.SpscPipe();
        pipe.Dispose();
    }

    [Fact]
    public void DoubleDispose_NoThrow()
    {
        var pipe = new SpscPipelines.SpscPipe();
        pipe.Dispose();
        pipe.Dispose();
    }

    [Fact]
    public async Task Dispose_AfterUse_ReleasesSegments()
    {
        var pipe = new SpscPipelines.SpscPipe();
        pipe.Writer.GetMemory(100); pipe.Writer.Advance(100);
        await pipe.Writer.FlushAsync();
        var r = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r.Buffer.End);

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Dispose();

        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.GetMemory(0));
        Assert.Throws<ObjectDisposedException>(() => pipe.Reader.TryRead(out _));
    }

    [Fact]
    public void CancelPendingRead_AfterDispose_Throws()
    {
        var pipe = new SpscPipelines.SpscPipe();
        pipe.Dispose();
        Assert.Throws<ObjectDisposedException>(() => pipe.Reader.CancelPendingRead());
    }

    [Fact]
    public void CancelPendingFlush_AfterDispose_Throws()
    {
        var pipe = new SpscPipelines.SpscPipe();
        pipe.Dispose();
        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.CancelPendingFlush());
    }
}
