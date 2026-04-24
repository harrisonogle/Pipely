using System.IO.Pipelines;

namespace SpscPipe.Benchmarks;

public enum Impl { SpscPipe, BclPipe }

public readonly record struct PipeConfig(
    int  MinimumSegmentSize,
    long PauseWriterThreshold,
    long ResumeWriterThreshold);

internal interface IPipeAdapter : IDisposable
{
    PipeReader Reader { get; }
    PipeWriter Writer { get; }
    void ResetOrRebuild();
}

internal static class AdapterFactory
{
    public static IPipeAdapter Build(Impl impl, PipeConfig cfg) => impl switch
    {
        Impl.SpscPipe => new SpscPipeAdapter(cfg),
        Impl.BclPipe  => new BclPipeAdapter(cfg),
        _             => throw new ArgumentOutOfRangeException(nameof(impl)),
    };
}

internal sealed class SpscPipeAdapter : IPipeAdapter
{
    private readonly global::SpscPipe.SpscPipe _pipe;

    public SpscPipeAdapter(PipeConfig cfg) => _pipe = Build(cfg);

    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;

    public void ResetOrRebuild() => _pipe.Reset();

    public void Dispose() => _pipe.Dispose();

    private static global::SpscPipe.SpscPipe Build(PipeConfig cfg) =>
        new global::SpscPipe.SpscPipe(new global::SpscPipe.SpscPipeOptions
        {
            MinimumSegmentSize    = cfg.MinimumSegmentSize,
            PauseWriterThreshold  = cfg.PauseWriterThreshold,
            ResumeWriterThreshold = cfg.ResumeWriterThreshold,
        });
}

internal sealed class BclPipeAdapter : IPipeAdapter
{
    private readonly Pipe _pipe;

    public BclPipeAdapter(PipeConfig cfg) => _pipe = Build(cfg);

    public PipeReader Reader => _pipe.Reader;
    public PipeWriter Writer => _pipe.Writer;

    // BCL Pipe.Reset() requires both sides to have completed. Caller (test or
    // [IterationCleanup]) must Complete reader+writer before calling. Reset
    // (rather than allocating a new Pipe) keeps per-iteration teardown cost
    // comparable to SpscPipeAdapter.ResetOrRebuild.
    public void ResetOrRebuild() => _pipe.Reset();

    public void Dispose()
    {
        // BCL Pipe is not IDisposable; nothing to do beyond letting GC reclaim it.
    }

    private static Pipe Build(PipeConfig cfg) =>
        new Pipe(new PipeOptions(
            pool: null,
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool,
            pauseWriterThreshold: cfg.PauseWriterThreshold,
            resumeWriterThreshold: cfg.ResumeWriterThreshold,
            minimumSegmentSize: cfg.MinimumSegmentSize,
            useSynchronizationContext: false));
}
