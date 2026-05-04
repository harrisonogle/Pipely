using BenchmarkDotNet.Attributes;
using System.IO.Pipelines;

namespace PipelyBenchmarks;

// Cache-line / busy-poll headline benchmark. Each Pipe uses PipeScheduler.Inline
// and has backpressure disabled (pauseWriterThreshold: 0); a dedicated raw
// Thread pinned to a fixed CPU runs the producer, another runs the consumer
// (busy-polling on TryRead). Producer/consumer cores are configurable via
// the PIPELY_PRODUCER_CORE / PIPELY_CONSUMER_CORE env vars (defaults 0 and 2).
//
// The same PinnedPipeRunner class drives `dotnet run -- cache-bench` for
// focused `perf c2c record` cache-line attribution outside BDN.
[MemoryDiagnoser]
public class PinnedThroughputBenchmarks
{
    private const int TotalBytes = 1 << 20;
    private const int ChunkSize  = 4096;

    private static readonly int ProducerCore = ParseEnv("PIPELY_PRODUCER_CORE", defaultValue: 0);
    private static readonly int ConsumerCore = ParseEnv("PIPELY_CONSUMER_CORE", defaultValue: 2);

    private BclPipeAdapter?    _bcl;
    private PipelyPipeAdapter? _pipely;
    private PinnedPipeRunner?  _bclRunner;
    private PinnedPipeRunner?  _pipelyRunner;

    [GlobalSetup(Target = nameof(BCL_PinnedBusyPoll))]
    public void SetupBcl()
    {
        _bcl = new BclPipeAdapter(new PipeOptions(
            readerScheduler:           PipeScheduler.Inline,
            writerScheduler:           PipeScheduler.Inline,
            pauseWriterThreshold:      0,
            resumeWriterThreshold:     0,
            useSynchronizationContext: false));
        _bclRunner = new PinnedPipeRunner(_bcl, ProducerCore, ConsumerCore, TotalBytes, ChunkSize);
    }

    [GlobalCleanup(Target = nameof(BCL_PinnedBusyPoll))]
    public void CleanupBcl()
    {
        _bclRunner!.Dispose();
        _bcl!.Writer.Complete();
        _bcl.Reader.Complete();
        _bcl.Dispose();
    }

    [GlobalSetup(Target = nameof(Pipely_PinnedBusyPoll))]
    public void SetupPipely()
    {
        _pipely = new PipelyPipeAdapter(new Pipely.PipeOptions(
            readerScheduler:           PipeScheduler.Inline,
            writerScheduler:           PipeScheduler.Inline,
            pauseWriterThreshold:      0,
            resumeWriterThreshold:     0,
            useSynchronizationContext: false));
        _pipelyRunner = new PinnedPipeRunner(_pipely, ProducerCore, ConsumerCore, TotalBytes, ChunkSize);
    }

    [GlobalCleanup(Target = nameof(Pipely_PinnedBusyPoll))]
    public void CleanupPipely()
    {
        _pipelyRunner!.Dispose();
        _pipely!.Writer.Complete();
        _pipely.Reader.Complete();
        _pipely.Dispose();
    }

    [Benchmark(Baseline = true)]
    public void BCL_PinnedBusyPoll() => _bclRunner!.RunOnce();

    [Benchmark]
    public void Pipely_PinnedBusyPoll() => _pipelyRunner!.RunOnce();

    private static int ParseEnv(string name, int defaultValue)
    {
        var v = Environment.GetEnvironmentVariable(name);
        return v is { Length: > 0 } && int.TryParse(v, out var parsed) ? parsed : defaultValue;
    }
}
