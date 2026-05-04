using BenchmarkDotNet.Running;
using PipelyBenchmarks;
using System.CommandLine;

// Subcommand-style routing:
//   (no args, or BDN args like --filter / --job)  → BenchmarkSwitcher (BDN auto-discovery)
//   latency                                       → BCL-vs-Pipe latency harness
//   cache-bench                                   → pinned busy-poll harness for `perf c2c`
if (args.Length == 0 || (args[0] != "latency" && args[0] != "cache-bench"))
{
    BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    return 0;
}

var rootCommand = new RootCommand("Benchmark harness")
{
    BuildLatencyCommand(),
    BuildCacheBenchCommand(),
};

return await rootCommand.Parse(args).InvokeAsync();

static Command BuildLatencyCommand()
{
    var countOption = new Option<int>("--count")
    {
        Description = "Message count",
        DefaultValueFactory = _ => 100_000,
    };

    var sizeOption = new Option<int>("--size")
    {
        Description = "Message size in bytes",
        DefaultValueFactory = _ => 256,
    };

    var trialsOption = new Option<int>("--trials")
    {
        Description = "Trial count",
        DefaultValueFactory = _ => 1,
    };

    var warmupOption = new Option<int>("--warmup")
    {
        Description = "Number of warmup trials run before recording (not included in stats)",
        DefaultValueFactory = _ => 0,
    };

    var continuousOption = new Option<bool>("--continuous")
    {
        Description = "Continuous-streaming mode: skip Writer.Complete() / Reader.Complete() so the consumer's final ReadAsync isn't waiting on an end-of-stream signal. Isolates steady-state tail behavior from end-of-stream artifacts.",
        DefaultValueFactory = _ => false,
    };

    var waitForAttachOption = new Option<bool>("--wait-for-attach")
    {
        Description = "Print PID then wait for Enter before running. Use to attach dotnet-trace before the benchmark starts (so the trace captures the actual run, not just the warmup/setup).",
        DefaultValueFactory = _ => false,
    };

    var latencyCommand = new Command("latency", "Run the BCL-vs-Pipe latency benchmark")
    {
        countOption,
        sizeOption,
        trialsOption,
        warmupOption,
        continuousOption,
        waitForAttachOption,
    };

    latencyCommand.SetAction(async parseResult =>
    {
        int count = parseResult.GetValue(countOption);
        int size = parseResult.GetValue(sizeOption);
        int trials = parseResult.GetValue(trialsOption);
        int warmup = parseResult.GetValue(warmupOption);
        bool continuous = parseResult.GetValue(continuousOption);
        bool waitForAttach = parseResult.GetValue(waitForAttachOption);

        if (waitForAttach)
        {
            Console.WriteLine($"PID: {Environment.ProcessId}");
            Console.WriteLine("Attach dotnet-trace now, then press Enter to begin benchmark...");
            Console.ReadLine();
        }

        await RunLatencyBenchmarks(count, size, trials, warmup, continuous);

        return 0;
    });

    return latencyCommand;
}

static Command BuildCacheBenchCommand()
{
    var pipeOption = new Option<string>("--pipe")
    {
        Description = "Pipe implementation: bcl or pipely",
        DefaultValueFactory = _ => "pipely",
    };

    var producerCoreOption = new Option<int>("--producer-core")
    {
        Description = "CPU index for the producer thread",
        DefaultValueFactory = _ => 0,
    };

    var consumerCoreOption = new Option<int>("--consumer-core")
    {
        Description = "CPU index for the consumer thread",
        DefaultValueFactory = _ => 2,
    };

    var durationOption = new Option<int>("--duration")
    {
        Description = "Run duration in seconds",
        DefaultValueFactory = _ => 10,
    };

    var totalBytesOption = new Option<int>("--total-bytes")
    {
        Description = "Bytes transferred per cycle",
        DefaultValueFactory = _ => 1 << 20,
    };

    var chunkOption = new Option<int>("--chunk")
    {
        Description = "Chunk size in bytes",
        DefaultValueFactory = _ => 4096,
    };

    var waitForAttachOption = new Option<bool>("--wait-for-attach")
    {
        Description = "Print PID then wait for Enter before running. Use to attach `perf c2c record -p <pid>` before the run.",
        DefaultValueFactory = _ => false,
    };

    var cmd = new Command("cache-bench", "Run a pinned busy-poll workload for `perf c2c` cache-line attribution (BCL or Pipely)")
    {
        pipeOption,
        producerCoreOption,
        consumerCoreOption,
        durationOption,
        totalBytesOption,
        chunkOption,
        waitForAttachOption,
    };

    cmd.SetAction(parseResult =>
    {
        string pipe = parseResult.GetValue(pipeOption) ?? "pipely";
        int producerCore = parseResult.GetValue(producerCoreOption);
        int consumerCore = parseResult.GetValue(consumerCoreOption);
        int duration     = parseResult.GetValue(durationOption);
        int totalBytes   = parseResult.GetValue(totalBytesOption);
        int chunkSize    = parseResult.GetValue(chunkOption);
        bool waitForAttach = parseResult.GetValue(waitForAttachOption);

        return RunCacheBench(pipe, producerCore, consumerCore, duration, totalBytes, chunkSize, waitForAttach);
    });

    return cmd;
}

static int RunCacheBench(string pipe, int producerCore, int consumerCore, int duration, int totalBytes, int chunkSize, bool waitForAttach)
{
    using IPipeAdapter adapter = pipe.ToLowerInvariant() switch
    {
        "bcl" => new BclPipeAdapter(new System.IO.Pipelines.PipeOptions(
            readerScheduler:           System.IO.Pipelines.PipeScheduler.Inline,
            writerScheduler:           System.IO.Pipelines.PipeScheduler.Inline,
            pauseWriterThreshold:      0,
            resumeWriterThreshold:     0,
            useSynchronizationContext: false)),
        "pipely" => new PipelyPipeAdapter(new Pipely.PipeOptions(
            readerScheduler:           System.IO.Pipelines.PipeScheduler.Inline,
            writerScheduler:           System.IO.Pipelines.PipeScheduler.Inline,
            pauseWriterThreshold:      0,
            resumeWriterThreshold:     0,
            useSynchronizationContext: false)),
        _ => throw new ArgumentException($"--pipe must be 'bcl' or 'pipely', was '{pipe}'"),
    };

    using var runner = new PinnedPipeRunner(adapter, producerCore, consumerCore, totalBytes, chunkSize);

    Console.WriteLine($"PID: {Environment.ProcessId}");
    Console.WriteLine($"Pipe: {pipe}  Producer cpu: {producerCore} (observed {runner.ProducerObservedCpu})  Consumer cpu: {consumerCore} (observed {runner.ConsumerObservedCpu})");
    Console.WriteLine($"Cycle: {totalBytes} bytes / {chunkSize}-byte chunks   Duration: {duration}s");

    if (waitForAttach)
    {
        Console.WriteLine("Attach `perf c2c record -p <PID>` (or similar) now, then press Enter to begin...");
        Console.ReadLine();
    }

    var sw = System.Diagnostics.Stopwatch.StartNew();
    long cycles = 0;
    var deadline = TimeSpan.FromSeconds(duration);
    while (sw.Elapsed < deadline)
    {
        runner.RunOnce();
        cycles++;
    }
    sw.Stop();

    double seconds = sw.Elapsed.TotalSeconds;
    double mibPerSec = (cycles * (double)totalBytes) / (1024 * 1024) / seconds;
    Console.WriteLine($"Cycles: {cycles}   Elapsed: {seconds:F3}s   Throughput: {mibPerSec:F1} MiB/s");

    return 0;
}

static async Task RunLatencyBenchmarks(int count, int size, int trials, int warmup, bool continuous)
{
    var samples = new LatencySamples(count);
    string modeSuffix = continuous ? ", continuous" : "";

    if (warmup > 0)
    {
        Console.WriteLine($"Running {warmup} warmup trial(s) of {count:N0} × {size} B{modeSuffix} (not recorded)...");
        for (int w = 1; w <= warmup; w++)
        {
            using (var bcl = new BclPipeAdapter())
                _ = await LatencyHarness.Run(bcl, samples, size, continuous);
            using (var spsc = new PipelyPipeAdapter())
                _ = await LatencyHarness.Run(spsc, samples, size, continuous);
        }
        Console.WriteLine("Warmup complete.");
    }

    if (trials == 1)
    {
        Console.WriteLine($"Running BCL Pipe ({count:N0} × {size} B{modeSuffix}).");
        LatencyStats bclStats;
        using (var bcl = new BclPipeAdapter())
            bclStats = await LatencyHarness.Run(bcl, samples, size, continuous);

        Console.WriteLine($"Running Pipely ({count:N0} × {size} B{modeSuffix}).");
        LatencyStats spscStats;
        AwaiterCounters spscReadCounters, spscFlushCounters;
        using (var spsc = new PipelyPipeAdapter())
        {
            spscStats = await LatencyHarness.Run(spsc, samples, size, continuous);
            spscReadCounters = spsc.GetReadAwaiterCounters();
            spscFlushCounters = spsc.GetFlushAwaiterCounters();
        }

        PrintTrialComparisons(bclStats, spscStats, spscReadCounters, spscFlushCounters);
    }
    else
    {
        Console.WriteLine($"Running {trials} trials of {count:N0} × {size} B per trial{modeSuffix} (BCL then Pipely each trial).");

        for (int t = 1; t <= trials; t++)
        {
            LatencyStats bclStats;
            using (var bcl = new BclPipeAdapter())
                bclStats = await LatencyHarness.Run(bcl, samples, size, continuous);

            LatencyStats spscStats;
            AwaiterCounters spscReadCounters, spscFlushCounters;
            using (var spsc = new PipelyPipeAdapter())
            {
                spscStats = await LatencyHarness.Run(spsc, samples, size, continuous);
                spscReadCounters = spsc.GetReadAwaiterCounters();
                spscFlushCounters = spsc.GetFlushAwaiterCounters();
            }

            Console.WriteLine();
            Console.WriteLine($"=== Trial {t}/{trials} ===");
            PrintTrialComparisons(bclStats, spscStats, spscReadCounters, spscFlushCounters);
        }
    }
}

static void PrintTrialComparisons(LatencyStats bclStats, LatencyStats spscStats, AwaiterCounters spscRead, AwaiterCounters spscFlush)
{
    LatencyHarness.PrintComparison(nameof(bclStats.Message), "BCL Pipe", bclStats.Message, "Pipely", spscStats.Message);
    LatencyHarness.PrintComparison(nameof(bclStats.Flush), "BCL Pipe", bclStats.Flush, "Pipely", spscStats.Flush);
    LatencyHarness.PrintComparison(nameof(bclStats.SyncFlush), "BCL Pipe", bclStats.SyncFlush, "Pipely", spscStats.SyncFlush);
    LatencyHarness.PrintComparison(nameof(bclStats.AsyncFlush), "BCL Pipe", bclStats.AsyncFlush, "Pipely", spscStats.AsyncFlush);
    LatencyHarness.PrintComparison(nameof(bclStats.Read), "BCL Pipe", bclStats.Read, "Pipely", spscStats.Read);
    LatencyHarness.PrintComparison(nameof(bclStats.SyncRead), "BCL Pipe", bclStats.SyncRead, "Pipely", spscStats.SyncRead);
    LatencyHarness.PrintComparison(nameof(bclStats.AsyncRead), "BCL Pipe", bclStats.AsyncRead, "Pipely", spscStats.AsyncRead);
    LatencyHarness.PrintComparisonValue(nameof(bclStats.MsgsPerRead), "BCL Pipe", bclStats.MsgsPerRead, "Pipely", spscStats.MsgsPerRead);

    // Wake-gap: time from continuation registered (OnCompleted) to continuation actually
    // running. The runtime's TP scheduling cost in isolation, per-await.
    LatencyHarness.PrintComparison(nameof(bclStats.WakeGapFlush), "BCL Pipe", bclStats.WakeGapFlush, "Pipely", spscStats.WakeGapFlush);
    LatencyHarness.PrintComparison(nameof(bclStats.WakeGapRead),  "BCL Pipe", bclStats.WakeGapRead,  "Pipe", spscStats.WakeGapRead);

    // TP work-item correlation (works for both pipes; ground-truth-via-counter for "did this call queue TP work?").
    LatencyHarness.PrintTpCorrelation("FlushTp",   "BCL Pipe", bclStats.FlushTp, "Pipely", spscStats.FlushTp);
    LatencyHarness.PrintTpCorrelation("ReadTp",    "BCL Pipe", bclStats.ReadTp,  "Pipe", spscStats.ReadTp);

    // SPSC-only awaiter diagnostics (precise park/signal/cancel counts from instrumented Interlocked counters).
    LatencyHarness.PrintAwaiterCounters("Pipely._readAwaiter",  spscRead);
    LatencyHarness.PrintAwaiterCounters("Pipely._flushAwaiter", spscFlush);
}
