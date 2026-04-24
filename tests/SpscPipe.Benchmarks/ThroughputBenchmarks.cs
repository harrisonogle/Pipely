using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;

namespace SpscPipe.Benchmarks;

[MemoryDiagnoser]
[ThreadingDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class ThroughputBenchmarks
{
    // Total bytes per timed op. Chosen so the smallest chunk (1 byte) still
    // completes in reasonable time and the largest (65536) doesn't finish in
    // microseconds. 1 MiB = 1M ops at chunk=1, 16 ops at chunk=65536.
    private const long TotalBytesPerOp   = 1L << 20;
    private const int  BatchPerFlushBulk = 16;

    // Tight thresholds for the Backpressure scenario. Producer/consumer both
    // run flat-out; small Pause forces frequent park/wake cycles. Spec §6.3.
    private const long BackpressurePause  = 4096;
    private const long BackpressureResume = 2048;

    [Params(Impl.SpscPipe, Impl.BclPipe)] public Impl Pipe;
    [Params(1, 16, 256, 4096, 65536)]      public int ChunkSize;

    // Declared as [Params] with a single value each so adding values to widen
    // the matrix is a one-line change. Currently locked to BCL PipeOptions
    // defaults for apples-to-apples comparison.
    [Params(4096)]   public int  MinimumSegmentSize;
    [Params(65536)]  public long PauseWriterThreshold;
    [Params(32768)]  public long ResumeWriterThreshold;

    private IPipeAdapter _pipe = null!;

    [IterationSetup(Targets = new[] { nameof(Bulk), nameof(Chatty) })]
    public void SetupNormal() =>
        _pipe = AdapterFactory.Build(Pipe, new PipeConfig(
            MinimumSegmentSize, PauseWriterThreshold, ResumeWriterThreshold));

    [IterationSetup(Target = nameof(Backpressure))]
    public void SetupBackpressure() =>
        _pipe = AdapterFactory.Build(Pipe, new PipeConfig(
            MinimumSegmentSize, BackpressurePause, BackpressureResume));

    [IterationCleanup]
    public void Cleanup() => _pipe.Dispose();

    [Benchmark] public Task Bulk()         => Run(BatchPerFlushBulk);
    [Benchmark] public Task Chatty()       => Run(batchPerFlush: 1);
    [Benchmark] public Task Backpressure() => Run(BatchPerFlushBulk);

    private Task Run(int batchPerFlush)
    {
        var producer = Workloads.BulkProducer(_pipe, ChunkSize, batchPerFlush, TotalBytesPerOp);
        var consumer = Workloads.DrainConsumer(_pipe);
        return Task.WhenAll(producer, consumer);
    }
}
