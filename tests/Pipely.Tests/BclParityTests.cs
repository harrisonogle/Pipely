using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using Xunit;

namespace PipelyTests;

public enum PipeKind { Bcl, Pipely }

public class BclParityTests
{
    private static (PipeReader Reader, PipeWriter Writer, IDisposable Disposer) CreatePipe(PipeKind kind, PipeOptions? bclOpts = null)
    {
        switch (kind)
        {
            case PipeKind.Bcl:
                var bcl = new Pipe(bclOpts ?? PipeOptions.Default);
                return (bcl.Reader, bcl.Writer, NoOpDisposable.Instance);
            case PipeKind.Pipely:
                var spsc = new Pipely.Pipe();    // defaults match BCL defaults
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

    private sealed class CapturingSynchronizationContext : SynchronizationContext
    {
        public int PostCount;
        public override void Post(SendOrPostCallback d, object? state)
        {
            Interlocked.Increment(ref PostCount);
            ThreadPool.UnsafeQueueUserWorkItem(_ => d(state), null);
        }
    }

    private static (PipeReader Reader, PipeWriter Writer, IDisposable Disposer) CreatePipeWithSyncCtx(PipeKind kind, bool useSyncCtx)
    {
        switch (kind)
        {
            case PipeKind.Bcl:
                var bcl = new Pipe(new PipeOptions(useSynchronizationContext: useSyncCtx));
                return (bcl.Reader, bcl.Writer, NoOpDisposable.Instance);
            case PipeKind.Pipely:
                var spsc = new Pipely.Pipe(new Pipely.PipeOptions { UseSynchronizationContext = useSyncCtx });
                return (spsc.Reader, spsc.Writer, spsc);
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Pipely)]
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
    [InlineData(PipeKind.Pipely)]
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
    [InlineData(PipeKind.Pipely)]
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
            var spsc = new Pipely.Pipe(new Pipely.PipeOptions(pauseWriterThreshold: pauseAt, resumeWriterThreshold: resumeAt));
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
    [InlineData(PipeKind.Pipely)]
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
    [InlineData(PipeKind.Pipely)]
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
    [InlineData(PipeKind.Pipely)]
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

    // NOTE: Originally documented as "PipelyCoalesces_BclThrows" — BCL Pipe historically threw
    // InvalidOperationException on a second Writer.Complete. As of .NET 10, BCL also coalesces
    // (verified empirically: a second Writer.Complete on a completed BCL Pipe is a no-op). This
    // test now asserts both pipes coalesce, eliminating a previously documented divergence.
    [Fact]
    public void DoubleComplete_BothCoalesce()
    {
        var bcl = new Pipe();
        bcl.Writer.Complete();
        bcl.Writer.Complete();   // no throw (BCL net10.0 coalesces)

        using var spsc = new Pipely.Pipe();
        spsc.Writer.Complete();
        spsc.Writer.Complete();   // no throw
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Pipely)]
    public async Task UnflushedBytes_ParityScript_AdvanceFlushCompleteSequence(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipe(kind);
        using (disp)
        {
            // Both BCL and Pipely override these to true.
            Assert.True(writer.CanGetUnflushedBytes);

            // Fresh pipe.
            Assert.Equal(0L, writer.UnflushedBytes);

            // Advance accumulates.
            writer.GetMemory(100);
            writer.Advance(40);
            Assert.Equal(40L, writer.UnflushedBytes);

            writer.Advance(10);
            Assert.Equal(50L, writer.UnflushedBytes);

            // FlushAsync resets to 0. Drain on the reader so backpressure
            // is irrelevant and the flush completes synchronously.
            var flushTask = writer.FlushAsync();
            var read = await reader.ReadAsync();
            reader.AdvanceTo(read.Buffer.End);
            await flushTask;
            Assert.Equal(0L, writer.UnflushedBytes);

            // Post-flush Advance accumulates from 0.
            writer.GetMemory(100);
            writer.Advance(7);
            Assert.Equal(7L, writer.UnflushedBytes);

            // Complete resets to 0 (BCL: CommitUnsynchronized inside CompleteWriter;
            // Pipely: snapshot publish in Complete writes _lastPublishedWriterState
            // to current _totalWritten).
            writer.Complete();
            Assert.Equal(0L, writer.UnflushedBytes);
        }
    }

    [Fact]
    public void UseSynchronizationContext_DefaultIsTrue()
    {
        Assert.True(Pipely.PipeOptions.Default.UseSynchronizationContext);
        Assert.True(new Pipely.PipeOptions().UseSynchronizationContext);
        Assert.True(new Pipely.PipeOptions { UseSynchronizationContext = true }.UseSynchronizationContext);
        Assert.False(new Pipely.PipeOptions { UseSynchronizationContext = false }.UseSynchronizationContext);
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Pipely)]
    public async Task UseSynchronizationContext_True_SCIsHonored(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipeWithSyncCtx(kind, useSyncCtx: true);
        using (disp)
        {
            var sc = new CapturingSynchronizationContext();
            var prev = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(sc);
            try
            {
                _ = Task.Run(async () =>
                {
                    await Task.Delay(50);
                    var mem = writer.GetMemory(5);
                    mem.Span.Clear();
                    writer.Advance(5);
                    await writer.FlushAsync();
                });

                var rr = await reader.ReadAsync();
                reader.AdvanceTo(rr.Buffer.End);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prev);
            }

            Assert.True(Volatile.Read(ref sc.PostCount) > 0,
                $"{kind}: expected SC.Post to be called when UseSynchronizationContext = true");
        }
    }

    [Theory]
    [InlineData(PipeKind.Bcl)]
    [InlineData(PipeKind.Pipely)]
    public async Task UseSynchronizationContext_False_SCIsBypassed(PipeKind kind)
    {
        var (reader, writer, disp) = CreatePipeWithSyncCtx(kind, useSyncCtx: false);
        using (disp)
        {
            var sc = new CapturingSynchronizationContext();
            var prev = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(sc);
            try
            {
                _ = Task.Run(async () =>
                {
                    await Task.Delay(50);
                    var mem = writer.GetMemory(5);
                    mem.Span.Clear();
                    writer.Advance(5);
                    await writer.FlushAsync();
                });

                var rr = await reader.ReadAsync();
                reader.AdvanceTo(rr.Buffer.End);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prev);
            }

            Assert.Equal(0, Volatile.Read(ref sc.PostCount));
        }
    }
}
