using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;

namespace PipelyBenchmarks;

[MemoryDiagnoser]
public class SchedulerBenchmarks
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize  = 4096;

    // Constructed once per benchmark run, reused across all iterations. This
    // matches the apples-to-apples comparison shape: BCL Pipe and Pipely with
    // PipeScheduler.ThreadPool both have zero per-iteration "scheduler" startup
    // cost (PipeScheduler.ThreadPool is a singleton). The FastScheduler
    // equivalent must also amortize its thread-startup cost across iterations
    // rather than pay it per measurement. Per-iteration cost is solely pipe
    // ctor + produce-and-drain on both sides.
    private Pipely.FastScheduler? _scheduler;

    [GlobalSetup(Target = nameof(Pipely_FastScheduler))]
    public void SetupFastScheduler() => _scheduler = new Pipely.FastScheduler();

    [GlobalCleanup(Target = nameof(Pipely_FastScheduler))]
    public void CleanupFastScheduler() => _scheduler?.Dispose();

    // BCL System.IO.Pipelines.Pipe with PipeScheduler.ThreadPool (the BCL
    // default, set explicitly for parallel framing with Pipely_ThreadPool).
    [Benchmark(Baseline = true)]
    public async Task BCL_ThreadPool()
    {
        var pipe = new Pipe(new PipeOptions(
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool));
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // BCL System.IO.Pipelines.Pipe with PipeScheduler.Inline — BCL itself
    // exposes the same Inline option; this row characterizes the BCL pipe's
    // own zero-thread-hop path for direct comparison with Pipely_Inline.
    [Benchmark]
    public async Task BCL_Inline()
    {
        var pipe = new Pipe(new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline));
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // Pipely with PipeScheduler.ThreadPool.
    [Benchmark]
    public async Task Pipely_ThreadPool()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions
        {
            ReaderScheduler = PipeScheduler.ThreadPool,
            WriterScheduler = PipeScheduler.ThreadPool,
        });
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // Pipely with PipeScheduler.Inline — continuations run synchronously on
    // the signaling thread (the producer for the read awaiter, the reader for
    // the flush awaiter). No thread hop on the signal path.
    [Benchmark]
    public async Task Pipely_Inline()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions
        {
            ReaderScheduler = PipeScheduler.Inline,
            WriterScheduler = PipeScheduler.Inline,
        });
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    // Pipely with FastScheduler (constructed once in [GlobalSetup], reused
    // across all iterations of this benchmark).
    [Benchmark]
    public async Task Pipely_FastScheduler()
    {
        using var pipe = new Pipely.Pipe(new Pipely.PipeOptions
        {
            ReaderScheduler = _scheduler,
            WriterScheduler = _scheduler,
        });
        await ProduceAndDrain(pipe.Reader, pipe.Writer);
    }

    private static async Task ProduceAndDrain(PipeReader reader, PipeWriter writer)
    {
        var producer = Task.Run(async () =>
        {
            int written = 0;
            var chunk = new byte[ChunkSize];
            while (written < TotalBytes)
            {
                var memory = writer.GetMemory(chunk.Length);
                chunk.CopyTo(memory);
                writer.Advance(chunk.Length);
                await writer.FlushAsync();
                written += chunk.Length;
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
