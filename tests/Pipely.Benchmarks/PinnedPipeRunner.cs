using System.Runtime.InteropServices;

namespace PipelyBenchmarks;

// Drives an IPipeAdapter under raw pinned threads, busy-polling on the consumer
// side (TryRead + tight loop) and never blocking on the producer side. Both
// threads are bound to fixed CPUs via sched_setaffinity (Linux only), so the
// cache-line activity observed by `perf c2c` reflects bouncing between *those
// two cores* — not ThreadPool migration or scheduler placement.
//
// The pipe must be configured to never park: PipeScheduler.Inline plus
// pauseWriterThreshold: 0 (disable backpressure). Otherwise the producer's
// FlushAsync will hit the awaiter path and the busy-poll guarantee is lost.
//
// Construction spawns the threads and waits until both have pinned successfully
// (so pin failure surfaces synchronously). Dispose joins them. RunOnce() runs
// one producer/consumer cycle of `totalBytes` and returns once both sides
// finish; intended to be called repeatedly on a long-lived runner (e.g. by BDN
// or a standalone harness driving a fixed-duration `perf c2c` capture).
internal sealed class PinnedPipeRunner : IDisposable
{
    private readonly IPipeAdapter _adapter;
    private readonly byte[] _chunk;
    private readonly int _totalBytes;

    private readonly Thread _producerThread;
    private readonly Thread _consumerThread;

    private readonly ManualResetEventSlim _producerReady = new(initialState: false);
    private readonly ManualResetEventSlim _consumerReady = new(initialState: false);
    private readonly ManualResetEventSlim _producerStart = new(initialState: false);
    private readonly ManualResetEventSlim _producerDone  = new(initialState: false);
    private readonly ManualResetEventSlim _consumerStart = new(initialState: false);
    private readonly ManualResetEventSlim _consumerDone  = new(initialState: false);
    private volatile int _exit;
    private Exception? _producerError;
    private Exception? _consumerError;

    public int ProducerCore { get; }
    public int ConsumerCore { get; }
    public int ProducerObservedCpu { get; private set; } = -1;
    public int ConsumerObservedCpu { get; private set; } = -1;

    public PinnedPipeRunner(IPipeAdapter adapter, int producerCore, int consumerCore, int totalBytes, int chunkSize)
    {
        ArgumentNullException.ThrowIfNull(adapter);
        if (totalBytes <= 0)             throw new ArgumentOutOfRangeException(nameof(totalBytes));
        if (chunkSize <= 0)              throw new ArgumentOutOfRangeException(nameof(chunkSize));
        if (totalBytes % chunkSize != 0) throw new ArgumentException("totalBytes must be a multiple of chunkSize", nameof(totalBytes));
        if (producerCore == consumerCore) throw new ArgumentException("Producer and consumer cores must differ");

        _adapter     = adapter;
        _chunk       = new byte[chunkSize];
        _totalBytes  = totalBytes;
        ProducerCore = producerCore;
        ConsumerCore = consumerCore;

        _producerThread = new Thread(() => ProducerLoop(producerCore))
        {
            IsBackground = true,
            Name = $"PinnedPipeRunner.Producer(cpu={producerCore})",
        };
        _consumerThread = new Thread(() => ConsumerLoop(consumerCore))
        {
            IsBackground = true,
            Name = $"PinnedPipeRunner.Consumer(cpu={consumerCore})",
        };
        _producerThread.Start();
        _consumerThread.Start();

        _producerReady.Wait();
        _consumerReady.Wait();
        if (_producerError is not null)
            throw new InvalidOperationException("Producer thread initialization failed", _producerError);
        if (_consumerError is not null)
            throw new InvalidOperationException("Consumer thread initialization failed", _consumerError);
    }

    public void RunOnce()
    {
        _producerError = null;
        _consumerError = null;
        _producerDone.Reset();
        _consumerDone.Reset();
        _producerStart.Set();
        _consumerStart.Set();
        _producerDone.Wait();
        _consumerDone.Wait();
        if (_producerError is not null)
            throw new InvalidOperationException("Producer threw during RunOnce", _producerError);
        if (_consumerError is not null)
            throw new InvalidOperationException("Consumer threw during RunOnce", _consumerError);
    }

    public void Dispose()
    {
        _exit = 1;
        _producerStart.Set();
        _consumerStart.Set();
        if (_producerThread.IsAlive) _producerThread.Join();
        if (_consumerThread.IsAlive) _consumerThread.Join();

        _producerReady.Dispose();
        _consumerReady.Dispose();
        _producerStart.Dispose();
        _producerDone.Dispose();
        _consumerStart.Dispose();
        _consumerDone.Dispose();
    }

    private void ProducerLoop(int cpu)
    {
        try
        {
            PinCurrentThreadToCpu(cpu);
            ProducerObservedCpu = sched_getcpu();
        }
        catch (Exception ex) { _producerError = ex; _producerReady.Set(); return; }
        _producerReady.Set();

        var writer = _adapter.Writer;
        var chunk  = _chunk;
        var total  = _totalBytes;

        while (true)
        {
            _producerStart.Wait();
            _producerStart.Reset();
            if (_exit != 0) return;

            try
            {
                int written = 0;
                while (written < total)
                {
                    var memory = writer.GetMemory(chunk.Length);
                    chunk.CopyTo(memory);
                    writer.Advance(chunk.Length);
                    var ft = writer.FlushAsync();
                    if (!ft.IsCompletedSuccessfully)
                        ft.AsTask().GetAwaiter().GetResult();
                    written += chunk.Length;
                }
            }
            catch (Exception ex) { _producerError = ex; }

            _producerDone.Set();
        }
    }

    private void ConsumerLoop(int cpu)
    {
        try
        {
            PinCurrentThreadToCpu(cpu);
            ConsumerObservedCpu = sched_getcpu();
        }
        catch (Exception ex) { _consumerError = ex; _consumerReady.Set(); return; }
        _consumerReady.Set();

        var reader = _adapter.Reader;
        var total  = _totalBytes;

        while (true)
        {
            _consumerStart.Wait();
            _consumerStart.Reset();
            if (_exit != 0) return;

            try
            {
                long read = 0;
                while (read < total)
                {
                    if (reader.TryRead(out var result))
                    {
                        read += result.Buffer.Length;
                        reader.AdvanceTo(result.Buffer.End);
                    }
                    // else: tight spin — keeps the consumer's L1 hot on the
                    // pipe's read-side state and produces the cache-line
                    // bouncing signal we want to measure.
                }
            }
            catch (Exception ex) { _consumerError = ex; }

            _consumerDone.Set();
        }
    }

    [DllImport("libc", EntryPoint = "sched_setaffinity", SetLastError = true)]
    private static extern int sched_setaffinity(int pid, UIntPtr cpusetsize, ref ulong mask);

    [DllImport("libc", EntryPoint = "sched_getcpu")]
    private static extern int sched_getcpu();

    private static void PinCurrentThreadToCpu(int cpu)
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            throw new PlatformNotSupportedException("PinnedPipeRunner requires Linux (sched_setaffinity).");
        if (cpu < 0 || cpu >= 64)
            throw new ArgumentOutOfRangeException(nameof(cpu), "Only CPUs 0..63 are supported by this harness.");

        ulong mask = 1UL << cpu;
        int rc = sched_setaffinity(0, (UIntPtr)sizeof(ulong), ref mask);
        if (rc != 0)
            throw new InvalidOperationException($"sched_setaffinity({cpu}) failed: errno={Marshal.GetLastPInvokeError()}");
    }
}
