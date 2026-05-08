using System.IO.Pipelines;
using BenchmarkDotNet.Attributes;

namespace PipelyBenchmarks;

// Headline four-way scheduler matrix. Each Pipe is constructed once in
// [GlobalSetup] and reused across all BDN iterations — neither side calls
// Complete() between iterations. Reported allocations exclude per-Pipe
// construction and isolate steady-state per-1 MiB-transfer cost across the
// four pipe × scheduler variants. This is the production-shape measurement
// (long-lived pipes) and drives the README's headline table.
//
// Pair with FreshPipeSchedulerBenchmarks: per-row (FreshPipe per-iter alloc) -
// (this row's per-iter alloc) ≈ amortized per-Pipe construction cost for
// that variant.
[MemoryDiagnoser]
public class SchedulerBenchmarks
{
    private const int TotalBytes = 1 << 20;
    private const int ChunkSize  = 4096;

    private readonly byte[] _chunk = new byte[ChunkSize];

    private Pipe?        _bclTp;
    private Pipe?        _bclInline;
    private Pipely.Pipe? _pipelyTp;
    private Pipely.Pipe? _pipelyInline;

    [GlobalSetup(Target = nameof(BCL_ThreadPool))]
    public void SetupBclTp() => _bclTp = new Pipe(new PipeOptions(
        readerScheduler: PipeScheduler.ThreadPool,
        writerScheduler: PipeScheduler.ThreadPool));

    [GlobalCleanup(Target = nameof(BCL_ThreadPool))]
    public void CleanupBclTp()
    {
        _bclTp!.Writer.Complete();
        _bclTp.Reader.Complete();
    }

    [GlobalSetup(Target = nameof(BCL_Inline))]
    public void SetupBclInline() => _bclInline = new Pipe(new PipeOptions(
        readerScheduler: PipeScheduler.Inline,
        writerScheduler: PipeScheduler.Inline));

    [GlobalCleanup(Target = nameof(BCL_Inline))]
    public void CleanupBclInline()
    {
        _bclInline!.Writer.Complete();
        _bclInline.Reader.Complete();
    }

    [GlobalSetup(Target = nameof(Pipely_ThreadPool))]
    public void SetupPipelyTp() => _pipelyTp = new Pipely.Pipe(new Pipely.PipeOptions(
        readerScheduler: PipeScheduler.ThreadPool,
        writerScheduler: PipeScheduler.ThreadPool));

    [GlobalCleanup(Target = nameof(Pipely_ThreadPool))]
    public void CleanupPipelyTp()
    {
        _pipelyTp!.Writer.Complete();
        _pipelyTp.Reader.Complete();
        _pipelyTp = null;   // Pipely.Pipe no longer IDisposable; let GC reclaim.
    }

    [GlobalSetup(Target = nameof(Pipely_Inline))]
    public void SetupPipelyInline() => _pipelyInline = new Pipely.Pipe(new Pipely.PipeOptions(
        readerScheduler: PipeScheduler.Inline,
        writerScheduler: PipeScheduler.Inline));

    [GlobalCleanup(Target = nameof(Pipely_Inline))]
    public void CleanupPipelyInline()
    {
        _pipelyInline!.Writer.Complete();
        _pipelyInline.Reader.Complete();
        _pipelyInline = null;
    }

    [Benchmark(Baseline = true)]
    public Task BCL_ThreadPool() => ProduceAndDrain(_bclTp!.Reader, _bclTp.Writer);

    [Benchmark]
    public Task BCL_Inline() => ProduceAndDrain(_bclInline!.Reader, _bclInline.Writer);

    [Benchmark]
    public Task Pipely_ThreadPool() => ProduceAndDrain(_pipelyTp!.Reader, _pipelyTp.Writer);

    [Benchmark]
    public Task Pipely_Inline() => ProduceAndDrain(_pipelyInline!.Reader, _pipelyInline.Writer);

    private Task ProduceAndDrain(PipeReader reader, PipeWriter writer)
    {
        var chunk = _chunk;

        var producer = Task.Run(async () =>
        {
            int written = 0;
            while (written < TotalBytes)
            {
                var memory = writer.GetMemory(chunk.Length);
                chunk.CopyTo(memory);
                writer.Advance(chunk.Length);
                await writer.FlushAsync();
                written += chunk.Length;
            }
            // No Complete — Pipe survives for the next iteration.
        });

        var consumer = Task.Run(async () =>
        {
            long read = 0;
            while (read < TotalBytes)
            {
                var result = await reader.ReadAsync();
                read += result.Buffer.Length;
                reader.AdvanceTo(result.Buffer.End);
            }
            // No Complete — Pipe survives for the next iteration.
        });

        return Task.WhenAll(producer, consumer);
    }
}
