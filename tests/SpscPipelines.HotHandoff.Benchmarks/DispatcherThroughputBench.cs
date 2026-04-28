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
    // fill) so BDN can measure the same workload the latency CLI is measuring
    // and we can isolate any methodology divergence between the two.
    //
    // Original throughput-shape configuration, for reversion:
    //     private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    //     private const int ChunkSize  = 4096;
    //     producer used `chunk.CopyTo(memory)` of a pre-allocated ChunkSize
    //     byte[] (zero-filled) — see git history for the throughput numbers
    //     (50.45 us HotHandoff / 70.34 us TpDefault / 105.09 us BCL).
    private const int MessageCount = 1_000_000;
    private const int ChunkSize    = 256;

    // Constructed once per benchmark run, reused across all iterations.
    private HotHandoffContinuationDispatcher? _dispatcher;

    // TEMPORARY latency instrumentation — replicates the latency CLI's
    // per-message processing inside BDN so we can measure the same workload
    // both ways and compare. Per-iteration P50/P99/Mean from this samples
    // array + per-iteration slot/TP deltas from the dispatcher's counters
    // are captured in [IterationCleanup] and printed in [GlobalCleanup].
    private long[] _samples = null!;
    private int _filledSamples;
    private long _prevSlot;
    private long _prevTp;
    private readonly List<IterStats> _iterStats = new();

    private record struct IterStats(long P50Ns, long P99Ns, long MeanNs, long SlotDispatched, long TpOverflowed);

    [GlobalSetup]
    public void SetupSamples()
    {
        _samples = new long[MessageCount];
        // Pre-touch every 4 KiB page (each long = 8 B → 512 longs per page).
        for (int i = 0; i < _samples.Length; i += 512) _samples[i] = 1;
        Array.Clear(_samples);
        _filledSamples = 0;
        _prevSlot = 0;
        _prevTp = 0;
        _iterStats.Clear();
    }

    [GlobalSetup(Target = nameof(SpscPipe_HotHandoff_ProduceAndDrain))]
    public void SetupHotHandoff() => _dispatcher = new HotHandoffContinuationDispatcher();

    [GlobalCleanup(Target = nameof(SpscPipe_HotHandoff_ProduceAndDrain))]
    public void CleanupHotHandoff() => _dispatcher?.Dispose();

    [IterationCleanup]
    public void CapturePerIteration()
    {
        if (_filledSamples == 0) { _iterStats.Add(default); return; }
        var span = _samples.AsSpan(0, _filledSamples);
        span.Sort();
        long freq = Stopwatch.Frequency;
        long p50  = TicksToNs(span[span.Length / 2], freq);
        long p99  = TicksToNs(span[Math.Min(span.Length - 1, (int)(span.Length * 0.99))], freq);
        double sum = 0;
        for (int i = 0; i < span.Length; i++) sum += span[i];
        long mean = TicksToNs((long)(sum / span.Length), freq);

        long slotDelta = 0, tpDelta = 0;
        if (_dispatcher is not null)
        {
            long currSlot = _dispatcher.SlotDispatchedCount;
            long currTp   = _dispatcher.TpOverflowedCount;
            slotDelta = currSlot - _prevSlot;
            tpDelta   = currTp   - _prevTp;
            _prevSlot = currSlot;
            _prevTp   = currTp;
        }

        _iterStats.Add(new IterStats(p50, p99, mean, slotDelta, tpDelta));
        _filledSamples = 0;
    }

    [GlobalCleanup]
    public void PrintPerIterationStats()
    {
        if (_iterStats.Count == 0) return;
        Console.WriteLine();
        Console.WriteLine($"--- Per-iteration latency + dispatch breakdown ({_iterStats.Count} iterations, including warmup) ---");
        Console.WriteLine($"| Iter |   P50 (ns) |   P99 (ns) |  Mean (ns) |       Slot |        TP |");
        Console.WriteLine($"|-----:|-----------:|-----------:|-----------:|-----------:|----------:|");
        for (int i = 0; i < _iterStats.Count; i++)
        {
            var s = _iterStats[i];
            Console.WriteLine($"| {i,4} | {s.P50Ns,10:N0} | {s.P99Ns,10:N0} | {s.MeanNs,10:N0} | {s.SlotDispatched,10:N0} | {s.TpOverflowed,9:N0} |");
        }
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
        _filledSamples = messageIdx;
    }

    private static long TicksToNs(long ticks, long freq) => (long)(ticks * 1_000_000_000.0 / freq);
}
