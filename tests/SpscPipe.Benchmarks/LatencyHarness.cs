using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace SpscPipe.Benchmarks;

internal sealed record LatencyStats(
    long MinTicks,
    long P50Ticks,
    long P90Ticks,
    long P99Ticks,
    long P999Ticks,
    long MaxTicks,
    double MeanTicks,
    long Frequency,
    int Messages,
    int MessageBytes
);

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
    public static async Task<LatencyStats> Run(IPipeAdapter adapter, long[] samples, int messageBytes)
    {
        if (messageBytes < 8) throw new ArgumentException("messageBytes must be >= 8 (8-byte timestamp prefix)");

        int messages = samples.Length;
        long bytesTotal = (long)messages * messageBytes;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < messages; i++)
            {
                var mem = adapter.Writer.GetMemory(messageBytes);
                long ticks = Stopwatch.GetTimestamp();
                MemoryMarshal.Write(mem.Span, in ticks);
                adapter.Writer.Advance(messageBytes);
                var fr = await adapter.Writer.FlushAsync();
                if (fr.IsCompleted) break;
            }
            adapter.Writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            long consumed = 0;
            int sampleIdx = 0;
            byte[] tsBuf = new byte[8];
            while (consumed < bytesTotal)
            {
                var rr = await adapter.Reader.ReadAsync();
                var buf = rr.Buffer;
                while (buf.Length >= messageBytes)
                {
                    buf.Slice(0, 8).CopyTo(tsBuf);
                    long sentTicks = MemoryMarshal.Read<long>(tsBuf);
                    long now = Stopwatch.GetTimestamp();
                    samples[sampleIdx++] = now - sentTicks;
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

        Array.Sort(samples);
        long freq = Stopwatch.Frequency;
        double meanTicks = 0;
        for (int i = 0; i < samples.Length; i++) meanTicks += samples[i];
        meanTicks /= samples.Length;

        return new LatencyStats(
            MinTicks:     samples[0],
            P50Ticks:     Percentile(samples, 0.50),
            P90Ticks:     Percentile(samples, 0.90),
            P99Ticks:     Percentile(samples, 0.99),
            P999Ticks:    Percentile(samples, 0.999),
            MaxTicks:     samples[^1],
            MeanTicks:    meanTicks,
            Frequency:    freq,
            Messages:     messages,
            MessageBytes: messageBytes);
    }

    // Side-by-side comparison table. "Ratio" column matches BDN convention:
    // ratio = comparison_mean / baseline_mean, formatted as decimal (e.g., 0.67 = SpscPipe 33% faster).
    // Baseline (BCL) is implicit at 1.00 by virtue of being the denominator.
    public static void PrintComparison(string baselineLabel, LatencyStats baseline, string compareLabel, LatencyStats compare)
    {
        Console.WriteLine($"| Stat   | {baselineLabel,14} | {compareLabel,14} | Ratio |");
        Console.WriteLine($"|:-------|---------------:|---------------:|------:|");
        PrintRow("Min",   baseline.MinTicks,   compare.MinTicks,   baseline.Frequency, compare.Frequency);
        PrintRow("P50",   baseline.P50Ticks,   compare.P50Ticks,   baseline.Frequency, compare.Frequency);
        PrintRow("P90",   baseline.P90Ticks,   compare.P90Ticks,   baseline.Frequency, compare.Frequency);
        PrintRow("P99",   baseline.P99Ticks,   compare.P99Ticks,   baseline.Frequency, compare.Frequency);
        PrintRow("P99.9", baseline.P999Ticks,  compare.P999Ticks,  baseline.Frequency, compare.Frequency);
        PrintRow("Max",   baseline.MaxTicks,   compare.MaxTicks,   baseline.Frequency, compare.Frequency);
        PrintRow("Mean",  baseline.MeanTicks,  compare.MeanTicks,  baseline.Frequency, compare.Frequency);
    }

    private static void PrintRow(string label, double baselineTicks, double compareTicks, long baselineFreq, long compareFreq)
    {
        double baselineNs = TicksToNs(baselineTicks, baselineFreq);
        double compareNs = TicksToNs(compareTicks, compareFreq);
        double ratio = compareNs / baselineNs;
        Console.WriteLine($"| {label,-6} | {baselineNs,11:N0} ns | {compareNs,11:N0} ns | {ratio,5:F2} |");
    }

    // Nearest-rank percentile: index = ceil(p * n) - 1, clamped to [0, n-1].
    private static long Percentile(long[] sortedSamples, double p)
    {
        int n = sortedSamples.Length;
        int idx = Math.Min(n - 1, Math.Max(0, (int)Math.Ceiling(p * n) - 1));
        return sortedSamples[idx];
    }

    private static double TicksToNs(double ticks, long freq) => ticks * 1_000_000_000 / freq;
}
