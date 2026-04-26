using BenchmarkDotNet.Running;
using SpscPipe.Benchmarks;

if (args.Length > 0 && args[0] == "latency")
{
    int messages = 1_000_000;
    int messageBytes = 256;
    int trials = 1;
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
    if (args.Length > 3)
    {
        if (!int.TryParse(args[3], out trials) || trials <= 0)
        {
            Console.Error.WriteLine($"Invalid trial count: '{args[3]}' (expected positive integer)");
            return 1;
        }
    }

    // Allocate sample buffer once and reuse across all trials and both pipes.
    // Per-trial allocation would add ~80 MB / trial of GC pressure that could confound the very
    // runtime-drift we're trying to detect across trials.
    long[] samples = new long[messages];
    // Pre-touch every 4 KB page to commit physical memory before the timed runs.
    for (int i = 0; i < samples.Length; i += 512) samples[i] = 1;

    if (trials == 1)
    {
        Console.WriteLine($"Running BCL Pipe ({messages:N0} × {messageBytes} B)...");
        LatencyStats bclStats;
        using (var bcl = new BclPipeAdapter())
            bclStats = await LatencyHarness.Run(bcl, samples, messageBytes);

        Console.WriteLine($"Running SpscPipe ({messages:N0} × {messageBytes} B)...");
        LatencyStats spscStats;
        using (var spsc = new SpscPipeAdapter())
            spscStats = await LatencyHarness.Run(spsc, samples, messageBytes);

        Console.WriteLine();
        LatencyHarness.PrintComparison("BCL Pipe", bclStats, "SpscPipe", spscStats);
    }
    else
    {
        Console.WriteLine($"Running {trials} trials of {messages:N0} × {messageBytes} B per trial (BCL then SpscPipe each trial)...");

        for (int t = 1; t <= trials; t++)
        {
            LatencyStats bclStats;
            using (var bcl = new BclPipeAdapter())
                bclStats = await LatencyHarness.Run(bcl, samples, messageBytes);

            LatencyStats spscStats;
            using (var spsc = new SpscPipeAdapter())
                spscStats = await LatencyHarness.Run(spsc, samples, messageBytes);

            Console.WriteLine();
            Console.WriteLine($"=== Trial {t}/{trials} ===");
            LatencyHarness.PrintComparison("BCL Pipe", bclStats, "SpscPipe", spscStats);
        }
    }

    return 0;
}

BenchmarkSwitcher.FromAssembly(typeof(ThroughputBenchmarks).Assembly).Run(args);
return 0;
