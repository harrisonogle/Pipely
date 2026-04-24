using System.Buffers;
using System.IO.Pipelines;

namespace SpscPipe;

// Per spec §3.
public sealed class SpscPipeOptions
{
    public int MinimumSegmentSize { get; init; } = 4096;
    public long PauseWriterThreshold { get; init; } = 65536;
    public long ResumeWriterThreshold { get; init; } = 32768;
    public MemoryPool<byte>? Pool { get; init; } = null;
    public PipeScheduler? ReaderScheduler { get; init; } = PipeScheduler.ThreadPool;
    public PipeScheduler? WriterScheduler { get; init; } = PipeScheduler.ThreadPool;
    public bool UseSynchronizationContext { get; init; } = true;

    internal void Validate()
    {
        if (MinimumSegmentSize < 1)
            throw new ArgumentOutOfRangeException(nameof(MinimumSegmentSize));
        if (ResumeWriterThreshold < 0)
            throw new ArgumentOutOfRangeException(nameof(ResumeWriterThreshold));
        if (PauseWriterThreshold < ResumeWriterThreshold)
            throw new ArgumentOutOfRangeException(nameof(PauseWriterThreshold));
    }
}
