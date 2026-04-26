using System.Buffers;

namespace SpscPipelines;

public sealed class SpscPipeOptions
{
    public MemoryPool<byte> Pool { get; }
    public int  MinimumSegmentSize    { get; }
    public long PauseWriterThreshold  { get; }
    public long ResumeWriterThreshold { get; }
    public int  MaxFreelistSegments   { get; }

    public SpscPipeOptions(
        MemoryPool<byte>? pool = null,
        int  minimumSegmentSize    = 4096,
        long pauseWriterThreshold  = 65536,
        long resumeWriterThreshold = 32768,
        int  maxFreelistSegments   = 256)
    {
        if (minimumSegmentSize <= 0) throw new ArgumentOutOfRangeException(nameof(minimumSegmentSize));
        if (pauseWriterThreshold < 0) throw new ArgumentOutOfRangeException(nameof(pauseWriterThreshold));
        if (resumeWriterThreshold < 0) throw new ArgumentOutOfRangeException(nameof(resumeWriterThreshold));
        if (pauseWriterThreshold > 0 && resumeWriterThreshold > pauseWriterThreshold)
            throw new ArgumentException("ResumeWriterThreshold must be <= PauseWriterThreshold.", nameof(resumeWriterThreshold));
        if (maxFreelistSegments < 0) throw new ArgumentOutOfRangeException(nameof(maxFreelistSegments));

        Pool = pool ?? MemoryPool<byte>.Shared;
        MinimumSegmentSize    = minimumSegmentSize;
        PauseWriterThreshold  = pauseWriterThreshold;
        ResumeWriterThreshold = resumeWriterThreshold;
        MaxFreelistSegments   = maxFreelistSegments;
    }

    public static SpscPipeOptions Default { get; } = new();
}
