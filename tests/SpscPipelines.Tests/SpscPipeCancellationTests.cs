using System.Buffers;
using SpscPipelines;
using Xunit;

namespace SpscPipelines.Tests;

public class SpscPipeCancellationTests
{
    [Fact]
    public async Task CancelPendingRead_WhileNotParked_NextReadReturnsCanceled()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe.Reader.CancelPendingRead();

        var result = await pipe.Reader.ReadAsync();
        Assert.True(result.IsCanceled);
    }

    [Fact]
    public async Task CancelPendingRead_WhileParked_DeliversCanceled()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        pipe.Reader.CancelPendingRead();
        var result = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(result.IsCanceled);
    }

    [Fact]
    public async Task CancelPendingRead_FromThirdThread_DeliversStashBuffer()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var mem = pipe.Writer.GetMemory(3);
        mem.Span[0] = 1; mem.Span[1] = 2; mem.Span[2] = 3;
        pipe.Writer.Advance(3);
        await pipe.Writer.FlushAsync();

        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.GetPosition(1), r1.Buffer.End);

        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);
        await Task.Run(() => pipe.Reader.CancelPendingRead());

        var result = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(result.IsCanceled);
        Assert.Equal(2, result.Buffer.Length);
        Assert.Equal(new byte[] { 2, 3 }, result.Buffer.ToArray());
    }

    [Fact]
    public async Task ReadAsync_WithCanceledToken_Throws()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var cts = new CancellationTokenSource();
        cts.Cancel();
        // ThrowsAnyAsync allows TaskCanceledException (subtype of OperationCanceledException),
        // which is what ValueTask.FromCanceled surfaces — matches BCL semantics.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pipe.Reader.ReadAsync(cts.Token));
    }

    [Fact]
    public async Task ReadAsync_TokenCancelsWhileParked_Throws()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var cts = new CancellationTokenSource();
        var readTask = pipe.Reader.ReadAsync(cts.Token).AsTask();
        Assert.False(readTask.IsCompleted);

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await readTask.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task CancelPendingFlush_WhileParked_DeliversCanceled()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(pauseWriterThreshold: 50, resumeWriterThreshold: 25));
        pipe.Writer.GetMemory(100); pipe.Writer.Advance(100);
        var flushTask = pipe.Writer.FlushAsync().AsTask();
        Assert.False(flushTask.IsCompleted);

        await Task.Run(() => pipe.Writer.CancelPendingFlush());

        var result = await flushTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(result.IsCanceled);
    }
}
