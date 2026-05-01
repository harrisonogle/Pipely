using BenchmarkDotNet.Attributes;

namespace PipelyBenchmarks;

// Headline throughput benchmark. Constructs the Pipe once in [GlobalSetup]
// and reuses it across all BDN iterations — neither side calls Complete()
// between iterations. Reported allocations therefore exclude per-Pipe
// construction (TripleBuffers, awaiters, reader/writer wrappers, first
// BufferSegment) and isolate the per-1 MiB-transfer steady-state cost.
// This is the production-shape measurement (long-lived pipes).
//
// Pair with FreshPipeThroughputBenchmarks: (FreshPipe per-iter alloc) -
// (this benchmark's per-iter alloc) ≈ amortized per-Pipe construction cost.
[MemoryDiagnoser]
public class ThroughputBenchmarks
{
    private const int TotalBytes = 1 << 20;        // 1 MiB per iteration
    private const int ChunkSize = 4096;

    private readonly byte[] _chunk = new byte[ChunkSize];
    private BclPipeAdapter?    _bcl;
    private PipelyPipeAdapter? _pipely;

    [GlobalSetup(Target = nameof(BclPipe_ProduceAndDrain))]
    public void SetupBcl() => _bcl = new BclPipeAdapter();

    [GlobalCleanup(Target = nameof(BclPipe_ProduceAndDrain))]
    public void CleanupBcl()
    {
        _bcl!.Writer.Complete();
        _bcl.Reader.Complete();
        _bcl.Dispose();
    }

    [GlobalSetup(Target = nameof(Pipely_ProduceAndDrain))]
    public void SetupPipely() => _pipely = new PipelyPipeAdapter();

    [GlobalCleanup(Target = nameof(Pipely_ProduceAndDrain))]
    public void CleanupPipely()
    {
        _pipely!.Writer.Complete();
        _pipely.Reader.Complete();
        _pipely.Dispose();
    }

    [Benchmark(Baseline = true)]
    public Task BclPipe_ProduceAndDrain() => ProduceAndDrain(_bcl!);

    [Benchmark]
    public Task Pipely_ProduceAndDrain() => ProduceAndDrain(_pipely!);

    private async Task ProduceAndDrain(IPipeAdapter adapter)
    {
        var writer = adapter.Writer;
        var reader = adapter.Reader;
        var chunk  = _chunk;

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

        await Task.WhenAll(producer, consumer);
    }
}
