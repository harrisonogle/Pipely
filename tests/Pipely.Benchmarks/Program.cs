using BenchmarkDotNet.Running;
using Pipely.Benchmarks;
using System.CommandLine;

// Subcommand-style routing:
//   (no args, or BDN args like --filter / --job)  → BenchmarkSwitcher (BDN auto-discovery)
//   latency                                       → BCL-vs-Pipe latency harness
//   dispatcher-latency                            → tp-default vs FastScheduler latency harness
if (args.Length == 0 || (args[0] != "latency" && args[0] != "dispatcher-latency"))
{
    BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    return 0;
}

var rootCommand = new RootCommand("Benchmark harness")
{
    BuildLatencyCommand(),
    BuildDispatcherLatencyCommand(),
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

static Command BuildDispatcherLatencyCommand()
{
    var countOption = new Option<int>("--count")
    {
        Description = "Message count per latency trial",
        DefaultValueFactory = _ => 100_000,
    };

    var sizeOption = new Option<int>("--size")
    {
        Description = "Message size in bytes (>= 8)",
        DefaultValueFactory = _ => 256,
    };

    var trialsOption = new Option<int>("--trials")
    {
        Description = "Number of latency trials (each trial runs both configurations)",
        DefaultValueFactory = _ => 3,
    };

    var warmupOption = new Option<int>("--warmup")
    {
        Description = "Warmup trials before recording (not included in results)",
        DefaultValueFactory = _ => 1,
    };

    var copyChunkOption = new Option<bool>("--copy-chunk")
    {
        Description = "Producer copies a full message-sized zero-filled chunk into each rented buffer (matching DispatcherThroughputBench.ProduceAndDrain's `chunk.CopyTo(memory)` per-event memcpy). When omitted (default), only the 8-byte timestamp is written and remaining bytes are uninitialized — the high-rate per-message latency workload. Pair with --count 256 --size 4096 for apples-to-apples with the BDN throughput row.",
        DefaultValueFactory = _ => false,
    };

    var dispatcherLatencyCommand = new Command("dispatcher-latency", "Run the dispatcher latency comparison (tp-default vs FastScheduler)")
    {
        countOption, sizeOption, trialsOption, warmupOption, copyChunkOption,
    };
    dispatcherLatencyCommand.SetAction(async parseResult =>
    {
        int count      = parseResult.GetValue(countOption);
        int size       = parseResult.GetValue(sizeOption);
        int trials     = parseResult.GetValue(trialsOption);
        int warmup     = parseResult.GetValue(warmupOption);
        bool copyChunk = parseResult.GetValue(copyChunkOption);
        await RunDispatcherLatency(count, size, trials, warmup, copyChunk);
        return 0;
    });

    return dispatcherLatencyCommand;
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
            using (var spsc = new PipeAdapter())
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

        Console.WriteLine($"Running Pipe ({count:N0} × {size} B{modeSuffix}).");
        LatencyStats spscStats;
        AwaiterCounters spscReadCounters, spscFlushCounters;
        using (var spsc = new PipeAdapter())
        {
            spscStats = await LatencyHarness.Run(spsc, samples, size, continuous);
            spscReadCounters = spsc.GetReadAwaiterCounters();
            spscFlushCounters = spsc.GetFlushAwaiterCounters();
        }

        PrintTrialComparisons(bclStats, spscStats, spscReadCounters, spscFlushCounters);
    }
    else
    {
        Console.WriteLine($"Running {trials} trials of {count:N0} × {size} B per trial{modeSuffix} (BCL then Pipe each trial).");

        for (int t = 1; t <= trials; t++)
        {
            LatencyStats bclStats;
            using (var bcl = new BclPipeAdapter())
                bclStats = await LatencyHarness.Run(bcl, samples, size, continuous);

            LatencyStats spscStats;
            AwaiterCounters spscReadCounters, spscFlushCounters;
            using (var spsc = new PipeAdapter())
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
    LatencyHarness.PrintComparison(nameof(bclStats.Message), "BCL Pipe", bclStats.Message, "Pipe", spscStats.Message);
    LatencyHarness.PrintComparison(nameof(bclStats.Flush), "BCL Pipe", bclStats.Flush, "Pipe", spscStats.Flush);
    LatencyHarness.PrintComparison(nameof(bclStats.SyncFlush), "BCL Pipe", bclStats.SyncFlush, "Pipe", spscStats.SyncFlush);
    LatencyHarness.PrintComparison(nameof(bclStats.AsyncFlush), "BCL Pipe", bclStats.AsyncFlush, "Pipe", spscStats.AsyncFlush);
    LatencyHarness.PrintComparison(nameof(bclStats.Read), "BCL Pipe", bclStats.Read, "Pipe", spscStats.Read);
    LatencyHarness.PrintComparison(nameof(bclStats.SyncRead), "BCL Pipe", bclStats.SyncRead, "Pipe", spscStats.SyncRead);
    LatencyHarness.PrintComparison(nameof(bclStats.AsyncRead), "BCL Pipe", bclStats.AsyncRead, "Pipe", spscStats.AsyncRead);
    LatencyHarness.PrintComparisonValue(nameof(bclStats.MsgsPerRead), "BCL Pipe", bclStats.MsgsPerRead, "Pipe", spscStats.MsgsPerRead);

    // Wake-gap: time from continuation registered (OnCompleted) to continuation actually
    // running. The runtime's TP scheduling cost in isolation, per-await.
    LatencyHarness.PrintComparison(nameof(bclStats.WakeGapFlush), "BCL Pipe", bclStats.WakeGapFlush, "Pipe", spscStats.WakeGapFlush);
    LatencyHarness.PrintComparison(nameof(bclStats.WakeGapRead),  "BCL Pipe", bclStats.WakeGapRead,  "Pipe", spscStats.WakeGapRead);

    // TP work-item correlation (works for both pipes; ground-truth-via-counter for "did this call queue TP work?").
    LatencyHarness.PrintTpCorrelation("FlushTp",   "BCL Pipe", bclStats.FlushTp, "Pipe", spscStats.FlushTp);
    LatencyHarness.PrintTpCorrelation("ReadTp",    "BCL Pipe", bclStats.ReadTp,  "Pipe", spscStats.ReadTp);

    // SPSC-only awaiter diagnostics (precise park/signal/cancel counts from instrumented Interlocked counters).
    LatencyHarness.PrintAwaiterCounters("Pipe._readAwaiter",  spscRead);
    LatencyHarness.PrintAwaiterCounters("Pipe._flushAwaiter", spscFlush);
}

static async Task RunDispatcherLatency(int count, int size, int trials, int warmup, bool copyChunk)
{
    string writeMode = copyChunk ? "full chunk copy" : "timestamp-only writes";
    Console.WriteLine($"Dispatcher latency comparison: {count:N0} messages × {size} B, {trials} trials, {warmup} warmup, {writeMode}");

    // Phase 1 — tp-default. No FastScheduler exists during this phase, so the
    // TP-default measurement is not contaminated by a busy-spinning worker
    // thread eating a core.
    Console.WriteLine();
    Console.WriteLine($"--- Phase 1: tp-default ({warmup} warmup + {trials} recorded trials) ---");
    for (int w = 0; w < warmup; w++)
    {
        Console.WriteLine($"  Warmup {w + 1}/{warmup} (not recorded)");
        _ = await DispatcherLatencyHarness.Run(null, count, size, copyChunk);
    }
    var tpTrials = new DispatcherLatencyStats[trials];
    for (int t = 0; t < trials; t++)
        tpTrials[t] = await DispatcherLatencyHarness.Run(null, count, size, copyChunk);

    // Phase 2 — FastScheduler. Single scheduler amortized across warmup +
    // all recorded trials (matches the [GlobalSetup] amortization pattern
    // used by DispatcherThroughputBench.Pipe_FastScheduler_ProduceAndDrain).
    // Per-trial slot/TP counts are taken as deltas of the cumulative
    // scheduler counters between trial boundaries.
    Console.WriteLine();
    Console.WriteLine($"--- Phase 2: FastScheduler ({warmup} warmup + {trials} recorded trials, single scheduler) ---");
    var fsTrials       = new DispatcherLatencyStats[trials];
    var dispatchTrials = new (long slot, long tp)[trials];
    using (var scheduler = new Pipely.FastScheduler())
    {
        for (int w = 0; w < warmup; w++)
        {
            Console.WriteLine($"  Warmup {w + 1}/{warmup} (not recorded)");
            _ = await DispatcherLatencyHarness.Run(scheduler, count, size, copyChunk);
        }
        long prevSlot = scheduler.SlotDispatchedCount;
        long prevTp   = scheduler.TpOverflowedCount;
        for (int t = 0; t < trials; t++)
        {
            fsTrials[t] = await DispatcherLatencyHarness.Run(scheduler, count, size, copyChunk);
            long currSlot = scheduler.SlotDispatchedCount;
            long currTp   = scheduler.TpOverflowedCount;
            dispatchTrials[t] = (currSlot - prevSlot, currTp - prevTp);
            prevSlot = currSlot;
            prevTp   = currTp;
        }
    }

    // Phase 3 — print per-trial side-by-side comparison.
    for (int t = 0; t < trials; t++)
    {
        Console.WriteLine();
        Console.WriteLine($"=== Trial {t + 1}/{trials} ===");
        DispatcherLatencyHarness.PrintComparison("Message latency (ns)", tpTrials[t], fsTrials[t]);
        var (slot, tp) = dispatchTrials[t];
        PrintDispatchBreakdown(slot, tp);
    }
}

static void PrintDispatchBreakdown(long slot, long tp)
{
    long total = slot + tp;
    if (total == 0) return;
    double slotPct = 100.0 * slot / total;
    double tpPct   = 100.0 * tp   / total;
    Console.WriteLine();
    Console.WriteLine("=== FastScheduler dispatch breakdown ===");
    Console.WriteLine($"| Path                 |       Count |    %    |");
    Console.WriteLine($"|:---------------------|------------:|--------:|");
    Console.WriteLine($"| Slot (worker thread) | {slot,11:N0} | {slotPct,6:F2}% |");
    Console.WriteLine($"| TP overflow          | {tp,11:N0} | {tpPct,6:F2}% |");
    Console.WriteLine($"| Total                | {total,11:N0} | 100.00% |");
}
