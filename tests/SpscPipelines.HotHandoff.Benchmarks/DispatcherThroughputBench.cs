using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using SpscPipelines;

namespace SpscPipelines.HotHandoff.Benchmarks;

[MemoryDiagnoser]
public class DispatcherThroughputBench
{
    // TEMPORARY workload — matches the latency CLI's MHz-rate per-message run
    // (1 M messages × 256 B, timestamp-only producer writes, no chunk-copy
    // fill) so BDN can measure the same workload the latency CLI is measuring.
    //
    // Original throughput-shape configuration, for reversion:
    //     private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    //     private const int ChunkSize  = 4096;
    //     producer used `chunk.CopyTo(memory)` of a pre-allocated ChunkSize
    //     byte[] (zero-filled) — see git history for the throughput numbers
    //     (50.45 us HotHandoff / 70.34 us TpDefault / 105.09 us BCL).
    private const int MessageCount = 1_000_000;
    private const int ChunkSize    = 256;

    private HotHandoffContinuationDispatcher? _dispatcher;

    // TEMPORARY latency instrumentation — replicates the latency CLI's
    // per-message processing inside BDN so the workload matches exactly.
    // The samples[] is pre-allocated in [GlobalSetup] (8 MB once, not per
    // iteration) so MemoryDiagnoser still reports honest per-iteration
    // allocations from the workload itself. Aggregate stats are printed
    // once in [GlobalCleanup] at the end of all iterations.
    private long[] _samples = null!;
    private long _totalSamplesRecorded;

    [GlobalSetup]
    public void SetupSamples()
    {
        _samples = new long[MessageCount];
        // Pre-touch every 4 KiB page (each long = 8 B → 512 longs per page).
        for (int i = 0; i < _samples.Length; i += 512) _samples[i] = 1;
        Array.Clear(_samples);
        _totalSamplesRecorded = 0;
    }

    [GlobalSetup(Target = nameof(SpscPipe_HotHandoff_ProduceAndDrain))]
    public void SetupHotHandoff() => _dispatcher = new HotHandoffContinuationDispatcher();

    [GlobalCleanup(Target = nameof(SpscPipe_HotHandoff_ProduceAndDrain))]
    public void CleanupHotHandoff()
    {
        long slot = _dispatcher?.SlotDispatchedCount ?? 0;
        long tp   = _dispatcher?.TpOverflowedCount ?? 0;
        long total = slot + tp;
        _dispatcher?.Dispose();

        Console.WriteLine();
        Console.WriteLine($"--- HotHandoff cumulative dispatch breakdown over benchmark ---");
        Console.WriteLine($"  Slot (worker thread): {slot,12:N0}");
        Console.WriteLine($"  TP overflow:          {tp,12:N0}");
        Console.WriteLine($"  Total:                {total,12:N0}");
        if (total > 0)
            Console.WriteLine($"  Slot share:           {100.0 * slot / total,12:F2}%");
    }

    [GlobalCleanup]
    public void PrintLastIterationLatency()
    {
        // The samples[] array contains the LAST iteration's per-message
        // latencies (the consumer overwrites it each iteration). With BDN
        // running many measured iterations, this is the snapshot from the
        // final one — useful as a sanity check on the latency-CLI comparison
        // without involving iteration-boundary hooks (which proved fragile).
        long count = Volatile.Read(ref _totalSamplesRecorded);
        if (count == 0) return;

        // Sort just the populated portion; samples are reused across iterations.
        int n = (int)Math.Min(count, _samples.Length);
        var span = _samples.AsSpan(0, n);
        span.Sort();
        long freq = Stopwatch.Frequency;
        long min  = TicksToNs(span[0],                                      freq);
        long p50  = TicksToNs(span[n / 2],                                  freq);
        long p90  = TicksToNs(span[Math.Min(n - 1, (int)(n * 0.90))],       freq);
        long p99  = TicksToNs(span[Math.Min(n - 1, (int)(n * 0.99))],       freq);
        long p999 = TicksToNs(span[Math.Min(n - 1, (int)(n * 0.999))],      freq);
        long max  = TicksToNs(span[n - 1],                                  freq);
        double sum = 0;
        for (int i = 0; i < n; i++) sum += span[i];
        long mean = TicksToNs((long)(sum / n), freq);

        Console.WriteLine();
        Console.WriteLine($"--- Last-iteration per-message latency (n={n:N0}) ---");
        Console.WriteLine($"  Min   : {min,10:N0} ns");
        Console.WriteLine($"  P50   : {p50,10:N0} ns");
        Console.WriteLine($"  P90   : {p90,10:N0} ns");
        Console.WriteLine($"  P99   : {p99,10:N0} ns");
        Console.WriteLine($"  P99.9 : {p999,10:N0} ns");
        Console.WriteLine($"  Max   : {max,10:N0} ns");
        Console.WriteLine($"  Mean  : {mean,10:N0} ns");
    }

    // BCL System.IO.Pipelines.Pipe — TP-driven continuations, default options
    // (64K pause / 32K resume — same thresholds as SpscPipeOptions.Default).
    [Benchmark(Baseline = true)]
    public async Task BclPipe_ProduceAndDrain()
    {
        var pipe = new Pipe();
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // SpscPipe with the default ThreadPoolContinuationDispatcher (no override).
    [Benchmark]
    public async Task SpscPipe_TpDefault_ProduceAndDrain()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions
        {
            ContinuationDispatcher = null,
        });
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // SpscPipe with the HotHandoffContinuationDispatcher (constructed once
    // in [GlobalSetup], reused across all iterations of this benchmark).
    [Benchmark]
    public async Task SpscPipe_HotHandoff_ProduceAndDrain()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions
        {
            ContinuationDispatcher = _dispatcher,
        });
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    private async Task ProduceAndDrain(PipeReader reader, PipeWriter writer)
    {
        long bytesTotal = (long)MessageCount * ChunkSize;
        int messageIdx = 0;

        var producer = Task.Run(async () =>
        {
            // Latency CLI's timestamp-only producer pattern.
            for (int i = 0; i < MessageCount; i++)
            {
                var memory = writer.GetMemory(ChunkSize);
                long t = Stopwatch.GetTimestamp();
                MemoryMarshal.Write(memory.Span, in t);
                writer.Advance(ChunkSize);
                await writer.FlushAsync();
            }
            writer.Complete();
        });

        var consumer = Task.Run(async () =>
        {
            // Latency CLI's per-message-processing consumer pattern: read the
            // 8-byte timestamp out of each chunk, capture (now - sentTicks)
            // into the samples array. This is the work that perturbs the
            // latency-CLI workload relative to a pure drain — replicate it
            // here so BDN measures the exact same shape.
            long consumed = 0;
            byte[] tsBuf = new byte[8];
            while (consumed < bytesTotal)
            {
                var rr = await reader.ReadAsync();
                var buf = rr.Buffer;
                while (buf.Length >= ChunkSize)
                {
                    buf.Slice(0, 8).CopyTo(tsBuf);
                    long sentTicks = MemoryMarshal.Read<long>(tsBuf);
                    long now = Stopwatch.GetTimestamp();
                    _samples[messageIdx++] = now - sentTicks;
                    consumed += ChunkSize;
                    buf = buf.Slice(ChunkSize);
                }
                long consumedThisRead = rr.Buffer.Length - buf.Length;
                reader.AdvanceTo(rr.Buffer.GetPosition(consumedThisRead), rr.Buffer.End);
                if (rr.IsCompleted && consumed >= bytesTotal) break;
            }
            reader.Complete();
        });

        await Task.WhenAll(producer, consumer);
        Volatile.Write(ref _totalSamplesRecorded, messageIdx);
    }

    private static long TicksToNs(long ticks, long freq) => (long)(ticks * 1_000_000_000.0 / freq);
}
