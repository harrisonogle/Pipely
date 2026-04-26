using BenchmarkDotNet.Running;
using SpscPipe.Benchmarks;

if (args.Length > 0 && args[0] == "latency")
{
    int messages = 1_000_000;
    int messageBytes = 256;
    if (args.Length > 1)
    {
        if (!int.TryParse(args[1], out messages) || messages <= 0)
        {
            Console.Error.WriteLine($"Invalid message count: '{args[1]}' (expected positive integer)");
            return 1;
        }
    }
    if (args.Length > 2)
    {
        if (!int.TryParse(args[2], out messageBytes) || messageBytes < 8)
        {
            Console.Error.WriteLine($"Invalid message bytes: '{args[2]}' (expected integer >= 8)");
            return 1;
        }
    }

    Console.WriteLine("=== BCL Pipe ===");
    using (var bcl = new BclPipeAdapter())
        await LatencyHarness.Run(bcl, messages, messageBytes);

    Console.WriteLine();
    Console.WriteLine("=== SpscPipe ===");
    using (var spsc = new SpscPipeAdapter())
        await LatencyHarness.Run(spsc, messages, messageBytes);

    return 0;
}

BenchmarkSwitcher.FromAssembly(typeof(ThroughputBenchmarks).Assembly).Run(args);
return 0;
