using BenchmarkDotNet.Running;
using SpscPipe.Benchmarks;

if (args.Length > 0 && args[0] == "latency")
{
    const int messages = 100_000;
    const int messageBytes = 256;

    Console.WriteLine("=== BCL Pipe ===");
    using (var bcl = new BclPipeAdapter())
        await LatencyHarness.Run(bcl, messages, messageBytes);

    Console.WriteLine();
    Console.WriteLine("=== SpscPipe ===");
    using (var spsc = new SpscPipeAdapter())
        await LatencyHarness.Run(spsc, messages, messageBytes);

    return;
}

BenchmarkSwitcher.FromAssembly(typeof(ThroughputBenchmarks).Assembly).Run(args);
