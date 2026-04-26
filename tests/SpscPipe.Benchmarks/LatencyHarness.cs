using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace SpscPipe.Benchmarks;

internal static class LatencyHarness
{
    // Producer writes fixed-size messages with a Stopwatch timestamp in the first 8 bytes.
    // Consumer reads each message and records (now - timestamp) into a flat sample array.
    // No artificial pacing — measures producer→consumer hand-off latency under sustained throughput.
    // After both sides finish, samples are sorted and exact percentiles are computed by index.
    public static async Task Run(IPipeAdapter adapter, int messages, int messageBytes)
    {
        if (messageBytes < 8) throw new ArgumentException("messageBytes must be >= 8 (8-byte timestamp prefix)");

        long[] samples = new long[messages];
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

        Console.WriteLine($"  Messages:    {messages:N0} × {messageBytes} B");
        Console.WriteLine($"  Min:         {TicksToNs(samples[0], freq):N0} ns");
        Console.WriteLine($"  P50:         {TicksToNs(Percentile(samples, 0.50), freq):N0} ns");
        Console.WriteLine($"  P90:         {TicksToNs(Percentile(samples, 0.90), freq):N0} ns");
        Console.WriteLine($"  P99:         {TicksToNs(Percentile(samples, 0.99), freq):N0} ns");
        Console.WriteLine($"  P99.9:       {TicksToNs(Percentile(samples, 0.999), freq):N0} ns");
        Console.WriteLine($"  Max:         {TicksToNs(samples[^1], freq):N0} ns");
        Console.WriteLine($"  Mean:        {meanTicks * 1_000_000_000 / freq:N0} ns");
    }

    // Nearest-rank percentile: index = ceil(p * n) - 1, clamped to [0, n-1].
    private static long Percentile(long[] sortedSamples, double p)
    {
        int n = sortedSamples.Length;
        int idx = Math.Min(n - 1, Math.Max(0, (int)Math.Ceiling(p * n) - 1));
        return sortedSamples[idx];
    }

    private static double TicksToNs(long ticks, long freq) => (double)ticks * 1_000_000_000 / freq;
}
