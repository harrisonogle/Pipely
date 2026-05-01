using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;

namespace PipelyBenchmarks;

internal sealed record DispatcherLatencyStats(
    long   Count,
    double MinNs,
    double P50Ns,
    double P90Ns,
    double P99Ns,
    double P999Ns,
    double MaxNs,
    double MeanNs);

internal static class DispatcherLatencyHarness
{
    // Producer writes fixed-size messages prefixed with a Stopwatch timestamp.
    // Consumer reads each message and records (now - timestamp). After both sides
    // finish, samples are sorted and exact percentiles are computed by index.
    //
    // dispatcher = null → Pipe uses the default ThreadPoolContinuationDispatcher.
    //
    // copyChunk = false (default): producer writes only the 8-byte timestamp;
    // remaining bytes in the rented buffer are uninitialized. This is the
    // high-rate per-message latency workload (e.g., 1 M × 256 B).
    //
    // copyChunk = true: producer also copies a pre-allocated zero-filled
    // message-sized chunk into the buffer, matching
    // SchedulerBenchmarks.ProduceAndDrain's `chunk.CopyTo(memory)` pattern.
    // Use with --count 256 --size 4096 for apples-to-apples with the BDN
    // throughput row.
    public static async Task<DispatcherLatencyStats> Run(System.IO.Pipelines.PipeScheduler? dispatcher, int messageCount, int messageBytes, bool copyChunk = false)
    {
        if (messageBytes < 8) throw new ArgumentException("messageBytes must be >= 8 (timestamp prefix)");

        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions(
            readerScheduler: dispatcher,
            writerScheduler: dispatcher));

        var samples = new long[messageCount];
        // Pre-touch every 4 KiB page to commit physical memory before the timed run.
        for (int i = 0; i < samples.Length; i += 512) samples[i] = 1;
        Array.Clear(samples);

        long bytesTotal = (long)messageCount * messageBytes;
        int messageIdx = 0;

        byte[]? chunk = copyChunk ? new byte[messageBytes] : null;

        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < messageCount; i++)
            {
                var mem = pipe.Writer.GetMemory(messageBytes);
                long t = Stopwatch.GetTimestamp();
                MemoryMarshal.Write(mem.Span, in t);
                if (chunk is not null)
                    chunk.AsSpan(8).CopyTo(mem.Span.Slice(8));
                pipe.Writer.Advance(messageBytes);
                var fr = await pipe.Writer.FlushAsync();
                if (fr.IsCompleted) break;
            }
            pipe.Writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            long consumed = 0;
            byte[] tsBuf = new byte[8];
            while (consumed < bytesTotal)
            {
                var rr = await pipe.Reader.ReadAsync();
                var buf = rr.Buffer;
                while (buf.Length >= messageBytes)
                {
                    buf.Slice(0, 8).CopyTo(tsBuf);
                    long sentTicks = MemoryMarshal.Read<long>(tsBuf);
                    long now = Stopwatch.GetTimestamp();
                    samples[messageIdx++] = now - sentTicks;
                    consumed += messageBytes;
                    buf = buf.Slice(messageBytes);
                }
                long consumedThisRead = rr.Buffer.Length - buf.Length;
                pipe.Reader.AdvanceTo(rr.Buffer.GetPosition(consumedThisRead), rr.Buffer.End);
                if (rr.IsCompleted && consumed >= bytesTotal) break;
            }
            pipe.Reader.Complete();
        });

        await Task.WhenAll(producer, consumer);

        var span = samples.AsSpan(0, messageIdx);
        span.Sort();
        long freq = Stopwatch.Frequency;
        double meanTicks = 0;
        for (int i = 0; i < span.Length; i++) meanTicks += span[i];
        meanTicks /= span.Length;

        return new DispatcherLatencyStats(
            Count:  span.Length,
            MinNs:  TicksToNs(span[0],                 freq),
            P50Ns:  TicksToNs(Percentile(span, 0.50),  freq),
            P90Ns:  TicksToNs(Percentile(span, 0.90),  freq),
            P99Ns:  TicksToNs(Percentile(span, 0.99),  freq),
            P999Ns: TicksToNs(Percentile(span, 0.999), freq),
            MaxNs:  TicksToNs(span[^1],                freq),
            MeanNs: TicksToNs(meanTicks,               freq));
    }

    public static void PrintComparison(string label, DispatcherLatencyStats baseline, DispatcherLatencyStats compare)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {label} ===");
        Console.WriteLine($"| {"Stat",-8} | {"ThreadPool",12} | {"FastScheduler",14} |  Ratio |");
        Console.WriteLine($"|:---------|-------------:|---------------:|-------:|");
        PrintRow("Count", baseline.Count,  compare.Count);
        PrintRow("Min",   baseline.MinNs,  compare.MinNs);
        PrintRow("P50",   baseline.P50Ns,  compare.P50Ns);
        PrintRow("P90",   baseline.P90Ns,  compare.P90Ns);
        PrintRow("P99",   baseline.P99Ns,  compare.P99Ns);
        PrintRow("P99.9", baseline.P999Ns, compare.P999Ns);
        PrintRow("Max",   baseline.MaxNs,  compare.MaxNs);
        PrintRow("Mean",  baseline.MeanNs, compare.MeanNs);
    }

    private static void PrintRow(string label, double baseline, double compare)
    {
        string ratio = baseline <= 0 ? "N/A" :
            string.Format(CultureInfo.InvariantCulture, "{0,6:F2}", compare / baseline);
        Console.WriteLine($"| {label,-8} | {baseline,11:N0}  | {compare,13:N0}  | {ratio} |");
    }

    private static long Percentile(Span<long> sorted, double p)
    {
        int n = sorted.Length;
        int idx = Math.Min(n - 1, Math.Max(0, (int)Math.Ceiling(p * n) - 1));
        return sorted[idx];
    }

    private static double TicksToNs(double ticks, long freq) => ticks * 1_000_000_000.0 / freq;
}
