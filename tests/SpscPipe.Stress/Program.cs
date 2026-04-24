using System.Diagnostics;
using SpscPipe.Stress;

// Randomized SPSC stress driver.  Two threads push/pull bytes through a
// SpscPipe; chaos-scheduling injects Thread.SpinWait delays at operation
// boundaries to expose memory-ordering bugs.  Each iteration has a hang
// timeout; byte-level correctness is verified end-to-end.
//
// Runs on x86_64 (local).  ARM64 CI is a follow-up: document the intent
// but not wired up in this session (ARM64 exhibits more reorderings than
// x86 TSO, so a dedicated CI job is the better stress vehicle).

int iterations = 100_000;
int seed = Random.Shared.Next();
int perIterTimeoutSec = 30;

for (var i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--iterations": iterations = int.Parse(args[++i]); break;
        case "--seed":       seed       = int.Parse(args[++i]); break;
        case "--timeout":    perIterTimeoutSec = int.Parse(args[++i]); break;
        case "--help":
        case "-h":
            Console.WriteLine("""
                SpscPipe.Stress
                  --iterations N    number of stress iterations (default 100_000)
                  --seed N          seed for the master RNG (default random)
                  --timeout S       per-iteration hang timeout in seconds (default 30)
                """);
            return 0;
    }
}

Console.WriteLine($"SpscPipe.Stress: iterations={iterations:N0} seed={seed} timeout={perIterTimeoutSec}s");

var sw = Stopwatch.StartNew();
var runner = new StressRunner(masterSeed: seed, perIterTimeout: TimeSpan.FromSeconds(perIterTimeoutSec));
var failure = runner.Run(iterations);
sw.Stop();

if (failure is null)
{
    Console.WriteLine($"OK: {iterations:N0} iterations in {sw.Elapsed.TotalSeconds:F1}s " +
                      $"({iterations / Math.Max(sw.Elapsed.TotalSeconds, 0.001):N0} iter/s)");
    return 0;
}
else
{
    Console.Error.WriteLine($"FAIL: iteration {failure.Iteration} seed {failure.IterSeed}");
    Console.Error.WriteLine(failure.Detail);
    return 1;
}
