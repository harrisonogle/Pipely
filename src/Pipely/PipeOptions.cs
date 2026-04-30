using System.Buffers;
using System.IO.Pipelines;

namespace Pipely;

public sealed class PipeOptions
{
    public MemoryPool<byte> Pool { get; }
    public int  MinimumSegmentSize    { get; }
    public long PauseWriterThreshold  { get; }
    public long ResumeWriterThreshold { get; }
    public int  MaxFreelistSegments   { get; }

    /// <summary>
    /// Routes parked-awaiter continuations to a thread of the scheduler's choosing.
    /// When null (the default), <see cref="PipeScheduler.ThreadPool"/> is used,
    /// which forwards to the .NET ThreadPool — observably identical to the prior
    /// <c>RunContinuationsAsynchronously = true</c> behavior. Init-only: chosen
    /// once at pipe construction. See the BCL <see cref="PipeScheduler"/> contract.
    /// </summary>
    public PipeScheduler? ContinuationDispatcher { get; init; }

    public PipeOptions(
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

    public static PipeOptions Default { get; } = new();
}
