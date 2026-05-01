using System.Buffers;
using System.IO.Pipelines;

namespace Pipely;

/// <summary>
/// Pipely options. Extends <see cref="System.IO.Pipelines.PipeOptions"/> with
/// <see cref="MaxFreelistSegments"/>; all other knobs are inherited and behave
/// identically to the BCL.
/// </summary>
public sealed class PipeOptions : System.IO.Pipelines.PipeOptions
{
    /// <summary>
    /// Cap on the number of <see cref="BufferSegment"/>s the writer keeps in
    /// each per-pipe freelist for reuse. Pipely-specific; no BCL analogue.
    /// </summary>
    public int MaxFreelistSegments { get; }

    public PipeOptions(
        MemoryPool<byte>? pool = null,
        PipeScheduler? readerScheduler = null,
        PipeScheduler? writerScheduler = null,
        long pauseWriterThreshold = 65536L,
        long resumeWriterThreshold = 32768L,
        int minimumSegmentSize = 4096,
        bool useSynchronizationContext = true,
        int maxFreelistSegments = 256)
        : base(
            pool: pool,
            readerScheduler: readerScheduler,
            writerScheduler: writerScheduler,
            pauseWriterThreshold: pauseWriterThreshold,
            resumeWriterThreshold: resumeWriterThreshold,
            minimumSegmentSize: minimumSegmentSize,
            useSynchronizationContext: useSynchronizationContext)
    {
        if (maxFreelistSegments < 0) throw new ArgumentOutOfRangeException(nameof(maxFreelistSegments));
        MaxFreelistSegments = maxFreelistSegments;
    }

    public static new PipeOptions Default { get; } = new();
}
