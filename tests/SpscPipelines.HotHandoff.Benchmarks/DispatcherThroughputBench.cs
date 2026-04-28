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
    // fill).
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

    [GlobalSetup(Target = nameof(SpscPipe_HotHandoff_ProduceAndDrain))]
    public void SetupHotHandoff() => _dispatcher = new HotHandoffContinuationDispatcher();

    [GlobalCleanup(Target = nameof(SpscPipe_HotHandoff_ProduceAndDrain))]
    public void CleanupHotHandoff()
    {
        // TEMPORARY diagnostic: print cumulative slot/TP-overflow counts at
        // the end of all HotHandoff iterations. This gives ground-truth
        // dispatch frequency at this workload, replacing the speculative
        // estimate from allocation counts.
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

    private static async Task ProduceAndDrain(PipeReader reader, PipeWriter writer)
    {
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
            while (true)
            {
                var result = await reader.ReadAsync();
                reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted) break;
            }
            reader.Complete();
        });

        await Task.WhenAll(producer, consumer);
    }
}
