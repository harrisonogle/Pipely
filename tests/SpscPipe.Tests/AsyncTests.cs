using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;

namespace SpscPipe.Tests;

public class AsyncTests
{
    [Fact]
    public async Task ReadAsync_Parks_UntilWriterFlushes()
    {
        using var pipe = new global::SpscPipe.SpscPipe();

        // Reader starts waiting.
        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        // Writer publishes — should wake the reader.
        var mem = pipe.Writer.GetMemory(2);
        mem.Span[0] = 0x11;
        mem.Span[1] = 0x22;
        pipe.Writer.Advance(2);
        var flushTask = pipe.Writer.FlushAsync().AsTask();
        Assert.True(flushTask.IsCompleted);   // writer sync-returns (no backpressure).

        // Reader wakes.
        var readResult = await readTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(2, (int)readResult.Buffer.Length);
        Assert.Equal(new byte[] { 0x11, 0x22 }, BuffersExt.ToArray(readResult.Buffer));
        pipe.Reader.AdvanceTo(readResult.Buffer.End);
    }

    [Fact]
    public async Task ReadAsync_Cancellable()
    {
        using var pipe = new global::SpscPipe.SpscPipe();
        using var cts = new CancellationTokenSource();

        var readTask = pipe.Reader.ReadAsync(cts.Token).AsTask();
        Assert.False(readTask.IsCompleted);

        cts.Cancel();

        var rr = await readTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(rr.IsCanceled);
    }

    [Fact]
    public async Task FlushAsync_Parks_AtPauseAndResumes_BelowResume()
    {
        using var pipe = new global::SpscPipe.SpscPipe(new SpscPipeOptions
        {
            MinimumSegmentSize = 8,
            PauseWriterThreshold = 8,
            ResumeWriterThreshold = 4,
        });

        // Fill enough to exceed Pause.
        var mem = pipe.Writer.GetMemory(16);
        for (var i = 0; i < 16; i++) mem.Span[i] = (byte)(i + 1);
        pipe.Writer.Advance(16);

        // Flush — should exceed Pause (outstanding = 16 >= 8) and park.
        var flushTask = pipe.Writer.FlushAsync().AsTask();
        Assert.False(flushTask.IsCompleted);   // parked.

        // Reader drains below Resume (consume 14 bytes; outstanding -> 2 < 4).
        Assert.True(pipe.Reader.TryRead(out var rr));
        Assert.Equal(16, (int)rr.Buffer.Length);
        var partial = rr.Buffer.Slice(0, 14);
        pipe.Reader.AdvanceTo(partial.End);

        var result = await flushTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.False(result.IsCanceled);
        Assert.False(result.IsCompleted);
    }
}
