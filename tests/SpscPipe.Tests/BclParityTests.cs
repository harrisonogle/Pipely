using System.Buffers;
using System.IO.Pipelines;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public enum PipeKind { Bcl, Spsc }

public class BclParityTests
{
    private static (PipeReader Reader, PipeWriter Writer, IDisposable Disposer) CreatePipe(PipeKind kind, PipeOptions? bclOpts = null)
    {
        switch (kind)
        {
            case PipeKind.Bcl:
                var bcl = new Pipe(bclOpts ?? PipeOptions.Default);
                return (bcl.Reader, bcl.Writer, NoOpDisposable.Instance);
            case PipeKind.Spsc:
                var spsc = new SpscPipelines.SpscPipe();    // defaults match BCL defaults
                return (spsc.Reader, spsc.Writer, spsc);
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private sealed class NoOpDisposable : IDisposable
    {
        public static readonly NoOpDisposable Instance = new();
        public void Dispose() { }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task WriterCompleteEx_NextReadAsyncThrows(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            var ex = new InvalidOperationException("test");
            writer.Complete(ex);
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.ReadAsync());
            Assert.Same(ex, thrown);
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task ReaderCompleteEx_NextFlushAsyncThrows(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            var ex = new InvalidOperationException("test");
            reader.Complete(ex);
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.FlushAsync());
            Assert.Same(ex, thrown);
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task BackpressureHysteresis_ParkAtPause_ResumeAtBelowResume(PipeKind kind)
    {
        const int pauseAt  = 100;
        const int resumeAt = 50;

        PipeReader reader; PipeWriter writer; IDisposable disp;
        if (kind == PipeKind.Bcl)
        {
            var bcl = new Pipe(new PipeOptions(pauseWriterThreshold: pauseAt, resumeWriterThreshold: resumeAt));
            reader = bcl.Reader; writer = bcl.Writer; disp = NoOpDisposable.Instance;
        }
        else
        {
            var spsc = new SpscPipelines.SpscPipe(new SpscPipeOptions(pauseWriterThreshold: pauseAt, resumeWriterThreshold: resumeAt));
            reader = spsc.Reader; writer = spsc.Writer; disp = spsc;
        }

        using (disp)
        {
            writer.GetMemory(150); writer.Advance(150);
            var flushTask = writer.FlushAsync().AsTask();

            var r = await reader.ReadAsync();
            reader.AdvanceTo(r.Buffer.GetPosition(125));   // 25 unconsumed, well below resume=50

            var result = await flushTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(result.IsCanceled);
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task EmptyPipe_TryReadReturnsFalse_ReadAsyncParks(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            Assert.False(reader.TryRead(out _));
            var t = reader.ReadAsync().AsTask();
            Assert.False(t.IsCompleted);
            writer.Complete();
            await t.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task RoundTripBytes_PreservesContent(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            var data = Enumerable.Range(0, 1000).Select(i => (byte)i).ToArray();
            var mem = writer.GetMemory(data.Length);
            data.CopyTo(mem);
            writer.Advance(data.Length);
            await writer.FlushAsync();
            writer.Complete();

            var rr = await reader.ReadAsync();
            Assert.Equal(data, rr.Buffer.ToArray());
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Spsc)]
    public async Task ReadAsync_TwiceWithoutAdvanceTo_BothThrow(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            writer.GetMemory(5); writer.Advance(5);
            await writer.FlushAsync();

            await reader.ReadAsync();
            Assert.Throws<InvalidOperationException>(() => reader.ReadAsync());
        }
    }

    // NOTE: Originally documented as "SpscCoalesces_BclThrows" — BCL Pipe historically threw
    // InvalidOperationException on a second Writer.Complete. As of .NET 10, BCL also coalesces
    // (verified empirically: a second Writer.Complete on a completed BCL Pipe is a no-op). This
    // test now asserts both pipes coalesce, eliminating a previously documented divergence.
    [Fact]
    public void DoubleComplete_BothCoalesce()
    {
        var bcl = new Pipe();
        bcl.Writer.Complete();
        bcl.Writer.Complete();   // no throw (BCL net10.0 coalesces)

        using var spsc = new SpscPipelines.SpscPipe();
        spsc.Writer.Complete();
        spsc.Writer.Complete();   // no throw
    }
}
