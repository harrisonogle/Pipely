using SpscPipe.Benchmarks;

namespace SpscPipe.Tests;

public class PipeAdapterTests
{
    [Theory]
    [InlineData(Impl.SpscPipe)]
    [InlineData(Impl.BclPipe)]
    public async Task RoundTrip_Smoke_BothImpls(Impl impl)
    {
        var cfg = new PipeConfig(MinimumSegmentSize: 4096,
                                 PauseWriterThreshold: 65536,
                                 ResumeWriterThreshold: 32768);
        using var pipe = AdapterFactory.Build(impl, cfg);

        var producer = Task.Run(async () =>
        {
            pipe.Writer.GetMemory(64);
            pipe.Writer.Advance(64);
            await pipe.Writer.FlushAsync();
            pipe.Writer.Complete();
        });

        long total = 0;
        var consumer = Task.Run(async () =>
        {
            while (true)
            {
                var rr = await pipe.Reader.ReadAsync();
                total += rr.Buffer.Length;
                pipe.Reader.AdvanceTo(rr.Buffer.End);
                if (rr.IsCompleted && rr.Buffer.IsEmpty) break;
            }
            pipe.Reader.Complete();
        });

        await Task.WhenAll(producer, consumer).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(64, total);
    }

    [Fact]
    public async Task Reset_Or_Rebuild_Returns_Usable_Pipe_For_Both_Impls()
    {
        var cfg = new PipeConfig(4096, 65536, 32768);

        foreach (var impl in new[] { Impl.SpscPipe, Impl.BclPipe })
        {
            using var pipe = AdapterFactory.Build(impl, cfg);
            // First round.
            pipe.Writer.GetMemory(8); pipe.Writer.Advance(8);
            await pipe.Writer.FlushAsync();
            pipe.Writer.Complete();
            var rr = await pipe.Reader.ReadAsync();
            pipe.Reader.AdvanceTo(rr.Buffer.End);
            pipe.Reader.Complete();

            pipe.ResetOrRebuild();

            // After reset, the adapter is usable again.
            pipe.Writer.GetMemory(8); pipe.Writer.Advance(8);
            await pipe.Writer.FlushAsync();
            pipe.Writer.Complete();
            var rr2 = await pipe.Reader.ReadAsync();
            Assert.Equal(8, rr2.Buffer.Length);
            pipe.Reader.AdvanceTo(rr2.Buffer.End);
            pipe.Reader.Complete();
        }
    }
}
