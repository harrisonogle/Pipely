using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace SpscPipe.Benchmarks;

internal sealed record SampleStats(
    long Count,
    long MinTicks,
    long P50Ticks,
    long P90Ticks,
    long P99Ticks,
    long P999Ticks,
    long MaxTicks,
    double MeanTicks,
    long Frequency
);

internal sealed record LatencyStats(
    int Messages,
    int MessageBytes,
    SampleStats Message,
    SampleStats Flush,
    SampleStats SyncFlush,
    SampleStats AsyncFlush,
    SampleStats Read,
    SampleStats SyncRead,
    SampleStats AsyncRead
);

internal sealed class LatencySamples
{
    public LatencySamples(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);

        Count = count;
        Message = Initialize(count);
        Flush = Initialize(count);
        SyncFlush = Initialize(count);
        AsyncFlush = Initialize(count);
        Read = Initialize(count);
        SyncRead = Initialize(count);
        AsyncRead = Initialize(count);
    }

    public readonly int Count;
    public readonly long[] Message;
    public readonly long[] Flush;
    public readonly long[] SyncFlush;
    public readonly long[] AsyncFlush;
    public readonly long[] Read;
    public readonly long[] SyncRead;
    public readonly long[] AsyncRead;

    private static long[] Initialize(int count)
    {
        // Allocate sample buffer once and reuse across all trials and both pipes.
        // Per-trial allocation would add ~80 MB / trial of GC pressure that could confound the very
        // runtime-drift we're trying to detect across trials.
        long[] samples = new long[count];

        // Pre-touch every 4 KB page to commit physical memory before the timed runs.
        for (int i = 0; i < samples.Length; i += 512)
        {
            samples[i] = 1;
        }

        return samples;
    }
}

internal static class LatencyHarness
{
    // Producer writes fixed-size messages with a Stopwatch timestamp in the first 8 bytes.
    // Consumer reads each message and records (now - timestamp) into the caller-provided sample array.
    // No artificial pacing — measures producer→consumer hand-off latency under sustained throughput.
    // After both sides finish, samples are sorted and exact percentiles are computed by index.
    //
    // The sample buffer is caller-owned so it can be reused across trials without re-allocating
    // (which would add GC pressure that confounds the very runtime-drift we may be measuring).
    // Caller is responsible for pre-touching pages before the first call.
    public static async Task<LatencyStats> Run(IPipeAdapter adapter, LatencySamples samples, int messageBytes)
    {
        if (messageBytes < 8) throw new ArgumentException("messageBytes must be >= 8 (8-byte timestamp prefix)");

        int messages = samples.Count;
        long bytesTotal = (long)messages * messageBytes;

        int messageIdx = 0;
        int flushIdx = 0;
        int syncFlushIdx = 0;
        int asyncFlushIdx = 0;
        int readIdx = 0;
        int syncReadIdx = 0;
        int asyncReadIdx = 0;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < messages; i++)
            {
                var mem = adapter.Writer.GetMemory(messageBytes);
                long t0 = Stopwatch.GetTimestamp();
                MemoryMarshal.Write(mem.Span, in t0);
                adapter.Writer.Advance(messageBytes);
                var flushTask = adapter.Writer.FlushAsync();
                FlushResult fr;
                long flushTicks;
                if (flushTask.IsCompleted)
                {
                    fr = flushTask.Result;
                    flushTicks = Stopwatch.GetTimestamp();
                    samples.SyncFlush[syncFlushIdx++] = flushTicks - t0;
                }
                else
                {
                    fr = await flushTask;
                    flushTicks = Stopwatch.GetTimestamp();
                    samples.AsyncFlush[asyncFlushIdx++] = flushTicks - t0;
                }
                samples.Flush[flushIdx++] = flushTicks - t0;
                if (fr.IsCompleted) break;
            }
            adapter.Writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            long consumed = 0;
            byte[] tsBuf = new byte[8];
            while (consumed < bytesTotal)
            {
                long t0 = Stopwatch.GetTimestamp();
                ReadResult rr;
                long readTicks;
                ValueTask<ReadResult> readTask = adapter.Reader.ReadAsync();
                if (readTask.IsCompleted)
                {
                    rr = readTask.Result;
                    readTicks = Stopwatch.GetTimestamp();
                    samples.SyncRead[syncReadIdx++] = readTicks - t0;
                }
                else
                {
                    rr = await readTask;
                    readTicks = Stopwatch.GetTimestamp();
                    samples.AsyncRead[asyncReadIdx++] = readTicks - t0;
                }
                samples.Read[readIdx++] = readTicks - t0;
                var buf = rr.Buffer;
                while (buf.Length >= messageBytes)
                {
                    buf.Slice(0, 8).CopyTo(tsBuf);
                    long sentTicks = MemoryMarshal.Read<long>(tsBuf);
                    long now = Stopwatch.GetTimestamp();
                    samples.Message[messageIdx++] = now - sentTicks;
                    consumed += messageBytes;
                    buf = buf.Slice(messageBytes);
                }
                long consumedThisRead = rr.Buffer.Length - buf.Length;
                adapter.Reader.AdvanceTo(rr.Buffer.GetPosition(consumedThisRead), rr.Buffer.End);
                if (rr.IsCompleted && consumed >= bytesTotal) break;
            }
            adapter.Reader.Complete();
        });

        await Task.WhenAll(producer, consumer);

        var messageSamples = samples.Message.AsSpan(0, messageIdx);
        var flushSamples = samples.Flush.AsSpan(0, flushIdx);
        var syncFlushSamples = samples.SyncFlush.AsSpan(0, syncFlushIdx);
        var asyncFlushSamples = samples.AsyncFlush.AsSpan(0, asyncFlushIdx);
        var readSamples = samples.Read.AsSpan(0, readIdx);
        var syncReadSamples = samples.SyncRead.AsSpan(0, syncReadIdx);
        var asyncReadSamples = samples.AsyncRead.AsSpan(0, asyncReadIdx);

        var result = new LatencyStats(
            Messages: messages,
            MessageBytes: messageBytes,
            Message: ComputeStatistics(messageSamples),
            Flush: ComputeStatistics(flushSamples),
            SyncFlush: ComputeStatistics(syncFlushSamples),
            AsyncFlush: ComputeStatistics(asyncFlushSamples),
            Read: ComputeStatistics(readSamples),
            SyncRead: ComputeStatistics(syncReadSamples),
            AsyncRead: ComputeStatistics(asyncReadSamples)
        );

        messageSamples.Clear();
        flushSamples.Clear();
        syncFlushSamples.Clear();
        asyncFlushSamples.Clear();
        readSamples.Clear();
        syncReadSamples.Clear();
        asyncReadSamples.Clear();

        return result;

        SampleStats ComputeStatistics(Span<long> samples)
        {
            if (samples.Length > 0)
            {
                samples.Sort();
                long freq = Stopwatch.Frequency;
                double meanTicks = 0;
                for (int i = 0; i < samples.Length; i++) meanTicks += samples[i];
                meanTicks /= samples.Length;

                return new SampleStats(
                    Count: samples.Length,
                    MinTicks: samples[0],
                    P50Ticks: Percentile(samples, 0.50),
                    P90Ticks: Percentile(samples, 0.90),
                    P99Ticks: Percentile(samples, 0.99),
                    P999Ticks: Percentile(samples, 0.999),
                    MaxTicks: samples[^1],
                    MeanTicks: meanTicks,
                    Frequency: freq);
            }
            else
            {
                return new SampleStats(
                    Count: samples.Length,
                    MinTicks: -1,
                    P50Ticks: -1,
                    P90Ticks: -1,
                    P99Ticks: -1,
                    P999Ticks: -1,
                    MaxTicks: -1,
                    MeanTicks: -1,
                    Frequency: -1);
            }
        }
    }

    // Side-by-side comparison table. "Ratio" column matches BDN convention:
    // ratio = comparison_mean / baseline_mean, formatted as decimal (e.g., 0.67 = SpscPipe 33% faster).
    // Baseline (BCL) is implicit at 1.00 by virtue of being the denominator.
    public static void PrintComparison(string statName, string baselineLabel, SampleStats baseline, string compareLabel, SampleStats compare)
    {
        Console.WriteLine();
        Console.WriteLine($"| {statName,-12} | {baselineLabel,12} | {compareLabel,12} |  Ratio |");
        Console.WriteLine($"|:-------------|-------------:|-------------:|-------:|");
        PrintRow("Count", baseline.Count, compare.Count);
        PrintRow("Min", baseline.MinTicks, compare.MinTicks, baseline.Frequency, compare.Frequency);
        PrintRow("P50", baseline.P50Ticks, compare.P50Ticks, baseline.Frequency, compare.Frequency);
        PrintRow("P90", baseline.P90Ticks, compare.P90Ticks, baseline.Frequency, compare.Frequency);
        PrintRow("P99", baseline.P99Ticks, compare.P99Ticks, baseline.Frequency, compare.Frequency);
        PrintRow("P99.9", baseline.P999Ticks, compare.P999Ticks, baseline.Frequency, compare.Frequency);
        PrintRow("Max", baseline.MaxTicks, compare.MaxTicks, baseline.Frequency, compare.Frequency);
        PrintRow("Mean", baseline.MeanTicks, compare.MeanTicks, baseline.Frequency, compare.Frequency);
    }

    private static void PrintRow(string label, double baselineTicks, double compareTicks, long baselineFreq, long compareFreq)
    {
        double baselineNs = TicksToNs(baselineTicks, baselineFreq);
        double compareNs = TicksToNs(compareTicks, compareFreq);
        string ratio = compareNs < 0 || baselineNs < 0
            ? "N/A"
            : string.Format(CultureInfo.InvariantCulture, "{0,6:F2}", compareNs / baselineNs);
        Console.WriteLine($"| {label,-12} | {baselineNs,11:N0}  | {compareNs,11:N0}  | {ratio} |");
    }

    private static void PrintRow(string label, double baselineValue, double compareValue)
    {
        string ratio = compareValue < 0 || baselineValue < 0
            ? "N/A"
            : string.Format(CultureInfo.InvariantCulture, "{0,6:F2}", compareValue / baselineValue);
        Console.WriteLine($"| {label,-12} | {baselineValue,11:N0}  | {compareValue,11:N0}  | {ratio} |");
    }

    // Nearest-rank percentile: index = ceil(p * n) - 1, clamped to [0, n-1].
    private static long Percentile(Span<long> sortedSamples, double p)
    {
        int n = sortedSamples.Length;
        int idx = Math.Min(n - 1, Math.Max(0, (int)Math.Ceiling(p * n) - 1));
        return sortedSamples[idx];
    }

    private static double TicksToNs(double ticks, long freq) => ticks * 1_000_000_000 / freq;
}
