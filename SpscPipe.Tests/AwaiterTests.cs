using System.Buffers;
using System.IO.Pipelines;
using System.Text;

namespace SpscPipe.Tests;

// Checkpoint 3: awaiter coordination exercised across threads, plus
// cancellation via CancelPendingRead/Flush and CancellationToken.
public class AwaiterTests
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

    // The reader parks because no data exists yet; a producer on a separate
    // thread flushes, which must wake the reader via MaybeSignalReaderAwaiter.
    [Fact]
    public async Task ReadAsync_ParksWhenEmpty_WakesOnRemoteFlush()
    {
        var pipe = NewPipe();
        var payload = "wake-up"u8.ToArray();

        var readTask = Task.Run(async () =>
        {
            var result = await pipe.Reader.ReadAsync();
            var bytes = result.Buffer.ToArray();
            pipe.Reader.AdvanceTo(result.Buffer.End);
            return bytes;
        });

        // Give the reader a moment to actually park (it will arm the awaiter).
        await Task.Delay(50);

        await Task.Run(async () =>
        {
            Write(pipe.Writer, payload);
            await pipe.Writer.FlushAsync();
        });

        var received = await readTask.WaitAsync(DefaultTimeout);
        Assert.Equal(payload, received);
    }

    // The writer parks because outstanding >= PauseThreshold; a consumer
    // on a separate thread drains below ResumeThreshold, which must wake
    // the writer via MaybeSignalWriterAwaiter (with hysteresis).
    [Fact]
    public async Task FlushAsync_ParksAbovePause_WakesOnDrainBelowResume()
    {
        // Tight thresholds to make this fast and deterministic.
        var pipe = NewPipe(minSegmentSize: 64, pause: 128, resume: 32);

        // Fill past Pause.
        var chunk = new byte[256];
        Write(pipe.Writer, chunk);

        // First flush publishes 256 bytes (> Pause=128) — parks.
        var flushTask = pipe.Writer.FlushAsync().AsTask();

        // Give the flush a moment to actually park.
        await Task.Delay(50);
        Assert.False(flushTask.IsCompleted, "Flush should park when outstanding >= PauseThreshold.");

        // Drain.  Reader pulls everything.
        var reader = pipe.Reader;
        var read = await reader.ReadAsync().AsTask().WaitAsync(DefaultTimeout);
        Assert.Equal(256, read.Buffer.Length);
        reader.AdvanceTo(read.Buffer.End);   // retiredBytes = 256, well below Resume=32

        // Writer should wake — the reader's AdvanceTo signals via
        // MaybeSignalWriterAwaiter since outstanding is now 0 < 32.
        var flushResult = await flushTask.WaitAsync(DefaultTimeout);
        Assert.False(flushResult.IsCanceled);
        Assert.False(flushResult.IsCompleted);
    }

    // §8.5 hysteresis: a drain that only returns below Pause but not below
    // Resume must NOT wake the writer (would cause wake/park thrash).
    [Fact]
    public async Task FlushAsync_HysteresisAvoidsPrematureWake()
    {
        // Pause=128, Resume=64.  Writer parks at 256 outstanding.
        // Reader drains to ~200 (below Pause but above Resume) — writer
        // should stay parked.  Reader then drains further to trigger wake.
        var pipe = NewPipe(minSegmentSize: 16, pause: 128, resume: 64);

        var chunk = new byte[256];
        Write(pipe.Writer, chunk);
        var flushTask = pipe.Writer.FlushAsync().AsTask();

        await Task.Delay(50);
        Assert.False(flushTask.IsCompleted);

        // Partial drain: read 56 bytes (outstanding 200, still above Resume=64).
        // ReadAsync returns the full buffer; AdvanceTo with a partial consumed
        // position controls the actual retirement.
        var read1 = await pipe.Reader.ReadAsync().AsTask().WaitAsync(DefaultTimeout);
        Assert.Equal(256, read1.Buffer.Length);

        var partial = read1.Buffer.GetPosition(56);
        pipe.Reader.AdvanceTo(partial);

        // Writer still parked because outstanding = 200 > Resume = 64.
        await Task.Delay(50);
        Assert.False(flushTask.IsCompleted);

        // Drain remainder.
        var read2 = await pipe.Reader.ReadAsync().AsTask().WaitAsync(DefaultTimeout);
        pipe.Reader.AdvanceTo(read2.Buffer.End);

        await flushTask.WaitAsync(DefaultTimeout);
    }

    // CancelPendingRead wakes a parked reader with IsCanceled=true.
    [Fact]
    public async Task CancelPendingRead_WakesParkedReader()
    {
        var pipe = NewPipe();

        var readTask = pipe.Reader.ReadAsync().AsTask();
        await Task.Delay(50);
        Assert.False(readTask.IsCompleted);

        pipe.Reader.CancelPendingRead();

        var result = await readTask.WaitAsync(DefaultTimeout);
        Assert.True(result.IsCanceled);
        Assert.False(result.IsCompleted);
    }

    // CancelPendingFlush wakes a parked writer with IsCanceled=true.
    [Fact]
    public async Task CancelPendingFlush_WakesParkedWriter()
    {
        var pipe = NewPipe(minSegmentSize: 64, pause: 128, resume: 32);
        Write(pipe.Writer, new byte[256]);
        var flushTask = pipe.Writer.FlushAsync().AsTask();

        await Task.Delay(50);
        Assert.False(flushTask.IsCompleted);

        pipe.Writer.CancelPendingFlush();

        var result = await flushTask.WaitAsync(DefaultTimeout);
        Assert.True(result.IsCanceled);
        Assert.False(result.IsCompleted);
    }

    // CancellationToken-driven cancellation wakes a parked reader.
    [Fact]
    public async Task ReadAsync_CancellationToken_WakesReader()
    {
        var pipe = NewPipe();
        using var cts = new CancellationTokenSource();

        var readTask = pipe.Reader.ReadAsync(cts.Token).AsTask();
        await Task.Delay(50);
        Assert.False(readTask.IsCompleted);

        cts.Cancel();

        var result = await readTask.WaitAsync(DefaultTimeout);
        Assert.True(result.IsCanceled);
    }

    // Sanity: sync-path ReadAsync when data is already present shouldn't
    // park or schedule anything — the ValueTask should already be completed.
    [Fact]
    public async Task ReadAsync_SyncPathCompletesImmediately_WhenDataAvailable()
    {
        var pipe = NewPipe();
        Write(pipe.Writer, "sync"u8);
        await pipe.Writer.FlushAsync();

        var vt = pipe.Reader.ReadAsync();
        Assert.True(vt.IsCompleted);   // synchronously completed

        var result = await vt;
        Assert.Equal("sync"u8.ToArray(), result.Buffer.ToArray());
        pipe.Reader.AdvanceTo(result.Buffer.End);
    }

    // Cross-thread round-trip of a larger payload — stress the awaiter path
    // repeatedly under natural contention.
    [Fact]
    public async Task CrossThread_LargeRoundTrip_PreservesBytes()
    {
        var pipe = NewPipe(minSegmentSize: 256, pause: 8192, resume: 4096);

        const int totalBytes = 100_000;
        var expected = new byte[totalBytes];
        for (var i = 0; i < totalBytes; i++)
            expected[i] = (byte)(i % 251);

        var writerTask = Task.Run(async () =>
        {
            const int chunkSize = 137;
            for (var offset = 0; offset < totalBytes; offset += chunkSize)
            {
                var size = Math.Min(chunkSize, totalBytes - offset);
                Write(pipe.Writer, expected.AsSpan(offset, size));
                if (offset % (chunkSize * 7) == 0)
                    await pipe.Writer.FlushAsync();
            }
            await pipe.Writer.FlushAsync();
        });

        var readerTask = Task.Run(async () =>
        {
            var accumulated = new byte[totalBytes];
            var filled = 0;
            while (filled < totalBytes)
            {
                var result = await pipe.Reader.ReadAsync();
                foreach (var segment in result.Buffer)
                {
                    var toCopy = Math.Min(segment.Length, totalBytes - filled);
                    segment.Span.Slice(0, toCopy).CopyTo(accumulated.AsSpan(filled));
                    filled += toCopy;
                    if (filled == totalBytes) break;
                }
                pipe.Reader.AdvanceTo(result.Buffer.End);
            }
            return accumulated;
        });

        await writerTask.WaitAsync(DefaultTimeout);
        var got = await readerTask.WaitAsync(DefaultTimeout);
        Assert.Equal(expected, got);
    }
}
