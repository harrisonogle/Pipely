using System.Buffers;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeAdvanceToTests
{
    [Fact]
    public async Task AdvanceTo_PartialConsume_PreservesRemainder()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var mem = pipe.Writer.GetMemory(10);
        for (int i = 0; i < 10; i++) mem.Span[i] = (byte)i;
        pipe.Writer.Advance(10);
        await pipe.Writer.FlushAsync();

        var r1 = await pipe.Reader.ReadAsync();
        var consumed = r1.Buffer.GetPosition(5);
        pipe.Reader.AdvanceTo(consumed);

        var r2 = await pipe.Reader.ReadAsync();
        Assert.Equal(5, r2.Buffer.Length);
        Assert.Equal(new byte[] { 5, 6, 7, 8, 9 }, r2.Buffer.ToArray());
    }

    [Fact]
    public async Task AdvanceTo_FullConsume_NextReadHasEmptyBuffer_IfNoData()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var mem = pipe.Writer.GetMemory(10);
        for (int i = 0; i < 10; i++) mem.Span[i] = (byte)i;
        pipe.Writer.Advance(10);
        await pipe.Writer.FlushAsync();

        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.End);

        Assert.False(pipe.Reader.TryRead(out _));
    }

    [Fact]
    public async Task AdvanceTo_BackwardsConsumed_Throws()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var mem = pipe.Writer.GetMemory(10); pipe.Writer.Advance(10);
        await pipe.Writer.FlushAsync();
        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.GetPosition(5));

        var r2 = await pipe.Reader.ReadAsync();
        // Try to "advance" backwards (consumed before _totalConsumed).
        Assert.Throws<InvalidOperationException>(() => pipe.Reader.AdvanceTo(r1.Buffer.Start));
    }

    [Fact]
    public async Task AdvanceTo_OnEmptyBuffer_DoesNotThrow_WithDefaultPositions()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        // Simulate empty IsCompleted=true ReadResult by direct publish.
        pipe._writerTb.ProducerSlot() = new WriterState { IsCompleted = true };
        pipe._writerTb.Publish();

        var r = await pipe.Reader.ReadAsync();
        Assert.True(r.IsCompleted);
        Assert.True(r.Buffer.IsEmpty);

        // Should not throw.
        pipe.Reader.AdvanceTo(r.Buffer.Start, r.Buffer.End);
    }

    [Fact]
    public async Task AdvanceTo_FromDifferentPipe_Throws()
    {
        using var pipe1 = new SpscPipelines.SpscPipe();
        using var pipe2 = new SpscPipelines.SpscPipe();

        pipe1.Writer.GetMemory(5); pipe1.Writer.Advance(5);
        await pipe1.Writer.FlushAsync();
        var r1 = await pipe1.Reader.ReadAsync();

        // Try to AdvanceTo on pipe2 with positions from pipe1 — should throw (R4-7).
        Assert.Throws<InvalidOperationException>(() => pipe2.Reader.AdvanceTo(r1.Buffer.End));
    }
}
