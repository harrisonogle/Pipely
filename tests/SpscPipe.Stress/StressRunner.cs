using System.Buffers;
using System.Diagnostics;

namespace SpscPipe.Stress;

public sealed class Failure
{
    public int Iteration { get; init; }
    public int IterSeed { get; init; }
    public string Detail { get; init; } = "";
}

internal sealed class StressRunner
{
    private readonly int _masterSeed;
    private readonly TimeSpan _perIterTimeout;

    internal StressRunner(int masterSeed, TimeSpan perIterTimeout)
    {
        _masterSeed = masterSeed;
        _perIterTimeout = perIterTimeout;
    }

    internal Failure? Run(int iterations)
    {
        // Use the master seed to derive per-iteration seeds.  Each
        // iteration's seed is deterministic given the master, so failing
        // iterations can be re-run in isolation with their reported seed.
        var master = new Random(_masterSeed);

        for (var i = 0; i < iterations; i++)
        {
            var iterSeed = master.Next();
            var failure = RunOne(i, iterSeed);
            if (failure is not null)
            {
                return failure;
            }

            if (i > 0 && i % 10_000 == 0)
            {
                Console.WriteLine($"  {i:N0} iterations OK");
            }
        }

        return null;
    }

    private Failure? RunOne(int iteration, int iterSeed)
    {
        var rng = new Random(iterSeed);

        // Pick pipe options at random each iteration to widen coverage.
        var pause = rng.Next(8, 256);
        // Resume must be >= 1: with Resume=0 the reader's signal condition
        // (outstanding < Resume) is never satisfied, producing a real
        // deadlock if the writer parks.  The spec permits 0 but it's a
        // degenerate choice; avoid it in stress.
        var resume = rng.Next(1, pause + 1);
        var opts = new SpscPipeOptions
        {
            MinimumSegmentSize    = rng.Next(4, 64),
            PauseWriterThreshold  = pause,
            ResumeWriterThreshold = resume,
        };

        var totalBytes = rng.Next(0, 4096);
        var chaosBudget = rng.Next(0, 16);

        using var pipe = new global::SpscPipe.SpscPipe(opts);
        pipe.Diag = new DiagLog();
        using var cts = new CancellationTokenSource(_perIterTimeout);

        var producerRng = new Random(iterSeed ^ 0x1357);
        var consumerRng = new Random(iterSeed ^ 0x2468);

        Exception? producerEx = null;
        Exception? consumerEx = null;

        var producer = new Thread(() =>
        {
            try
            {
                RunProducerAsync(pipe, producerRng, totalBytes, chaosBudget, cts.Token)
                    .GetAwaiter().GetResult();
            }
            catch (Exception ex) { producerEx = ex; }
        })
        { IsBackground = true, Name = "producer" };

        var consumer = new Thread(() =>
        {
            try
            {
                RunConsumerAsync(pipe, consumerRng, totalBytes, chaosBudget, cts.Token)
                    .GetAwaiter().GetResult();
            }
            catch (Exception ex) { consumerEx = ex; }
        })
        { IsBackground = true, Name = "consumer" };

        producer.Start();
        consumer.Start();

        var joinedP = producer.Join(_perIterTimeout);
        var joinedC = consumer.Join(_perIterTimeout);

        if (!joinedP || !joinedC)
        {
            cts.Cancel();
            producer.Join(TimeSpan.FromSeconds(1));
            consumer.Join(TimeSpan.FromSeconds(1));
            return new Failure
            {
                Iteration = iteration,
                IterSeed = iterSeed,
                Detail = $"hang: producer-joined={joinedP} consumer-joined={joinedC} " +
                         $"totalBytes={totalBytes} opts=(seg={opts.MinimumSegmentSize}," +
                         $"pause={opts.PauseWriterThreshold},resume={opts.ResumeWriterThreshold})\n" +
                         $"producerEx: {producerEx}\n" +
                         $"consumerEx: {consumerEx}\n\n" +
                         $"DIAG TIMELINE:\n{pipe.Diag!.Dump()}"
            };
        }

        if (producerEx is not null)
        {
            return new Failure
            {
                Iteration = iteration, IterSeed = iterSeed,
                Detail = $"producer threw: {producerEx}"
            };
        }
        if (consumerEx is not null)
        {
            return new Failure
            {
                Iteration = iteration, IterSeed = iterSeed,
                Detail = $"consumer threw: {consumerEx}\n\nDIAG TIMELINE:\n{pipe.Diag!.Dump()}"
            };
        }

        return null;
    }

    // Producer: write a monotonic byte pattern where byte N = (byte)((N+1)&0xFF).
    // Consumer verifies each byte matches its position.  Async to avoid the
    // double-GetResult pitfall on IValueTaskSource-backed ValueTasks — the
    // consumer earlier converted VT → Task + called .Result, which invokes
    // GetResult twice and trips _readInProgress.
    private static async Task RunProducerAsync(global::SpscPipe.SpscPipe pipe, Random rng,
                                                int totalBytes, int chaosBudget, CancellationToken ct)
    {
        var written = 0;
        while (written < totalBytes && !ct.IsCancellationRequested)
        {
            var chunk = Math.Min(rng.Next(1, 32), totalBytes - written);
            var mem = pipe.Writer.GetMemory(chunk);
            var span = mem.Span;
            for (var i = 0; i < chunk; i++)
            {
                span[i] = (byte)((written + i + 1) & 0xFF);
            }
            pipe.Writer.Advance(chunk);
            written += chunk;

            Chaos(rng, chaosBudget);

            if (written == totalBytes || rng.Next(4) == 0)
            {
                var fr = await pipe.Writer.FlushAsync(ct).ConfigureAwait(false);
                if (fr.IsCompleted || fr.IsCanceled) break;
            }
        }
        pipe.Writer.Complete();
    }

    private static async Task RunConsumerAsync(global::SpscPipe.SpscPipe pipe, Random rng,
                                                int totalBytes, int chaosBudget, CancellationToken ct)
    {
        long read = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var rr = await pipe.Reader.ReadAsync(ct).ConfigureAwait(false);
            if (rr.IsCanceled) throw new InvalidOperationException("reader unexpectedly canceled");

            foreach (var mem in rr.Buffer)
            {
                var span = mem.Span;
                for (var i = 0; i < span.Length; i++)
                {
                    var expected = (byte)((read + i + 1) & 0xFF);
                    var actual = span[i];
                    if (expected != actual)
                    {
                        throw new InvalidDataException(
                            $"byte mismatch at position {read + i}: expected {expected}, got {actual}");
                    }
                }
                read += span.Length;
            }

            pipe.Reader.AdvanceTo(rr.Buffer.End);

            Chaos(rng, chaosBudget);

            if (rr.IsCompleted && rr.Buffer.IsEmpty) break;
        }

        if (read != totalBytes)
        {
            throw new InvalidDataException($"byte count mismatch: expected {totalBytes}, got {read}");
        }
        pipe.Reader.Complete();
    }

    // Chaos injection: randomly SpinWait to perturb scheduling.  Cheap and
    // avoids calling the OS scheduler (which would blunt the interleaving
    // coverage we want).
    private static void Chaos(Random rng, int budget)
    {
        if (budget == 0) return;
        var spins = rng.Next(0, budget + 1);
        if (spins > 0) Thread.SpinWait(spins * 16);
    }
}
