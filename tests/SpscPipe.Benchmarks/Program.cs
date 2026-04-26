using BenchmarkDotNet.Running;
using SpscPipe.Benchmarks;

if (args.Length > 0 && args[0] == "latency")
{
    using var adapter = new BclPipeAdapter();
    await LatencyHarness.Run(adapter, messages: 100_000, messageBytes: 256);
    return;
}

BenchmarkSwitcher.FromAssembly(typeof(ThroughputBenchmarks).Assembly).Run(args);
