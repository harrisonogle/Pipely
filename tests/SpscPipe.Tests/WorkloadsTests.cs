using SpscPipe.Benchmarks;

namespace SpscPipe.Tests;

public class WorkloadsTests
{
    [Theory]
    [InlineData(Impl.SpscPipe, 16,    1,     4096)]
    [InlineData(Impl.SpscPipe, 256,   16,    65536)]
    [InlineData(Impl.SpscPipe, 4096,  4,     65536)]
    [InlineData(Impl.BclPipe,  16,    1,     4096)]
    [InlineData(Impl.BclPipe,  256,   16,    65536)]
    [InlineData(Impl.BclPipe,  4096,  4,     65536)]
    public async Task BulkProducer_DrainConsumer_TransfersExactByteCount(
        Impl impl, int chunkSize, int batchPerFlush, long totalBytes)
    {
        var cfg = new PipeConfig(MinimumSegmentSize: 4096,
                                 PauseWriterThreshold: 65536,
                                 ResumeWriterThreshold: 32768);
        using var pipe = AdapterFactory.Build(impl, cfg);

        long observed = 0;
        var consumer = Task.Run(async () =>
        {
            while (true)
            {
                var rr = await pipe.Reader.ReadAsync();
                observed += rr.Buffer.Length;
                pipe.Reader.AdvanceTo(rr.Buffer.End);
                if (rr.IsCompleted && rr.Buffer.IsEmpty) break;
            }
            pipe.Reader.Complete();
        });

        var producer = Task.Run(() =>
            Workloads.BulkProducer(pipe, chunkSize, batchPerFlush, totalBytes));

        await Task.WhenAll(producer, consumer).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(totalBytes, observed);
    }
}
