using System.Buffers;
using System.IO.Pipelines;

namespace SpscPipe.Tests;

// Checkpoint 4: Complete / Dispose / Reset / finalizer / exception propagation.
public class LifecycleTests
{
    private static TimeSpan DefaultTimeout => TimeSpan.FromSeconds(5);

    private static global::SpscPipe.SpscPipe NewPipe(
        int minSegmentSize = 4096,
        long pause = 1024 * 1024,
        long resume = 512 * 1024)
    {
        return new global::SpscPipe.SpscPipe(new SpscPipeOptions
        {
            MinimumSegmentSize = minSegmentSize,
            PauseWriterThreshold = pause,
            ResumeWriterThreshold = resume,
        });
    }

    private static void Write(PipeWriter writer, ReadOnlySpan<byte> data)
    {
        var mem = writer.GetMemory(data.Length);
        data.CopyTo(mem.Span);
        writer.Advance(data.Length);
    }

    // Writer.Complete without data → reader sees IsCompleted=true empty.
    [Fact]
    public async Task WriterComplete_NoData_ReaderSeesIsCompleted()
    {
        var pipe = NewPipe();
        pipe.Writer.Complete();

        var result = await pipe.Reader.ReadAsync();
        Assert.True(result.IsCompleted);
        Assert.Equal(0, result.Buffer.Length);
        pipe.Reader.AdvanceTo(result.Buffer.End);
    }

    // Writer.Complete with unflushed bytes → bytes delivered, IsCompleted=true.
    [Fact]
    public async Task WriterComplete_WithUnflushedBytes_DeliversAndCompletes()
    {
        var pipe = NewPipe();
        Write(pipe.Writer, "goodbye"u8);
        pipe.Writer.Complete();   // should splice before setting completion

        var result = await pipe.Reader.ReadAsync();
        Assert.True(result.IsCompleted);
        Assert.Equal("goodbye"u8.ToArray(), result.Buffer.ToArray());
        pipe.Reader.AdvanceTo(result.Buffer.End);

        // Next read is empty+completed (no exception set).
        var final = await pipe.Reader.ReadAsync();
        Assert.True(final.IsCompleted);
        Assert.Equal(0, final.Buffer.Length);
        pipe.Reader.AdvanceTo(final.Buffer.End);
    }

    // Writer.Complete with exception → reader's ReadAsync throws AFTER the
    // buffered bytes are delivered (§10.4).
    [Fact]
    public async Task WriterComplete_WithException_ThrowsAfterDrain()
    {
        var pipe = NewPipe();
        Write(pipe.Writer, "partial"u8);
        pipe.Writer.Complete(new InvalidOperationException("writer-boom"));

        // First read delivers buffered bytes with IsCompleted=true, no throw.
        var data = await pipe.Reader.ReadAsync();
        Assert.True(data.IsCompleted);
        Assert.Equal("partial"u8.ToArray(), data.Buffer.ToArray());
        pipe.Reader.AdvanceTo(data.Buffer.End);

        // Second read throws the writer's exception.
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await pipe.Reader.ReadAsync());
        Assert.Equal("writer-boom", thrown.Message);
    }

    // Reader.Complete → writer's FlushAsync returns IsCompleted=true.
    [Fact]
    public async Task ReaderComplete_FlushAsyncSeesIsCompleted()
    {
        var pipe = NewPipe();
        pipe.Reader.Complete();

        Write(pipe.Writer, "anyone-there"u8);
        var result = await pipe.Writer.FlushAsync();
        Assert.True(result.IsCompleted);
    }

    // Reader.Complete with exception → writer's FlushAsync throws.
    [Fact]
    public async Task ReaderComplete_WithException_FlushAsyncThrows()
    {
        var pipe = NewPipe();
        pipe.Reader.Complete(new InvalidOperationException("reader-boom"));

        Write(pipe.Writer, "post-mortem"u8);
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await pipe.Writer.FlushAsync());
        Assert.Equal("reader-boom", thrown.Message);
    }

    // Writer.Complete wakes a parked reader.
    [Fact]
    public async Task WriterComplete_WakesParkedReader()
    {
        var pipe = NewPipe();

        var readTask = pipe.Reader.ReadAsync().AsTask();
        await Task.Delay(50);
        Assert.False(readTask.IsCompleted);

        pipe.Writer.Complete();

        var result = await readTask.WaitAsync(DefaultTimeout);
        Assert.True(result.IsCompleted);
        Assert.Equal(0, result.Buffer.Length);
    }

    // Reader.Complete wakes a parked writer, even above Resume threshold —
    // the hysteresis is bypassed when the reader has completed.
    [Fact]
    public async Task ReaderComplete_WakesParkedWriter_BypassesHysteresis()
    {
        var pipe = NewPipe(minSegmentSize: 64, pause: 128, resume: 32);

        Write(pipe.Writer, new byte[256]);
        var flushTask = pipe.Writer.FlushAsync().AsTask();

        await Task.Delay(50);
        Assert.False(flushTask.IsCompleted);

        // Reader hasn't drained anything — outstanding is 256, well above Resume=32.
        // The Complete should still wake the writer.
        pipe.Reader.Complete();

        var result = await flushTask.WaitAsync(DefaultTimeout);
        Assert.True(result.IsCompleted);
    }

    // Dispose after both completed does not throw.
    [Fact]
    public void Dispose_AfterBothCompleted_Succeeds()
    {
        var pipe = NewPipe();
        Write(pipe.Writer, "some-bytes"u8);
        pipe.Writer.Complete();
        pipe.Reader.Complete();

        pipe.Dispose();
        // Second Dispose is idempotent (should not throw even though
        // SuppressFinalize has run).
        pipe.Dispose();
    }

    // Dispose before anything happened.
    [Fact]
    public void Dispose_OnPristinePipe_Succeeds()
    {
        var pipe = NewPipe();
        pipe.Dispose();
    }

    // Reset before both ended throws.
    [Fact]
    public void Reset_BeforeBothCompleted_Throws()
    {
        var pipe = NewPipe();
        pipe.Writer.Complete();   // only writer — reader not completed

        Assert.Throws<InvalidOperationException>(() => pipe.Reset());
    }

    // Reset after both completed allows reuse.
    [Fact]
    public async Task Reset_AfterBothCompleted_AllowsReuse()
    {
        var pipe = NewPipe();

        Write(pipe.Writer, "first-run"u8);
        await pipe.Writer.FlushAsync();
        pipe.Writer.Complete();

        var r1 = await pipe.Reader.ReadAsync();
        Assert.Equal("first-run"u8.ToArray(), r1.Buffer.ToArray());
        pipe.Reader.AdvanceTo(r1.Buffer.End);
        var r1end = await pipe.Reader.ReadAsync();
        Assert.True(r1end.IsCompleted);
        pipe.Reader.AdvanceTo(r1end.Buffer.End);
        pipe.Reader.Complete();

        pipe.Reset();

        // Second run on the reset pipe.
        Write(pipe.Writer, "second-run"u8);
        await pipe.Writer.FlushAsync();
        pipe.Writer.Complete();

        var r2 = await pipe.Reader.ReadAsync();
        Assert.Equal("second-run"u8.ToArray(), r2.Buffer.ToArray());
        pipe.Reader.AdvanceTo(r2.Buffer.End);
        pipe.Reader.Complete();
    }

    // Finalizer runs without throwing on an abandoned pipe.
    // (Hard to test the finalizer directly; use GC.Collect + WaitForPendingFinalizers.)
    [Fact]
    public void Finalizer_OnAbandonedPipe_DoesNotThrow()
    {
        // Create, use briefly, drop reference.
        CreateAndAbandon();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // If the finalizer threw, the test host would typically fail.
        // Reaching here means the finalizer either ran cleanly or hasn't
        // run yet — both acceptable.
    }

    private static void CreateAndAbandon()
    {
        var pipe = NewPipe();
        // Write a little data to populate state, then abandon without Dispose.
        Write(pipe.Writer, "leaked"u8);
        // Do not call Complete or Dispose.
    }
}
