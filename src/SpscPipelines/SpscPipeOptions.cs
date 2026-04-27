using System.Buffers;

namespace SpscPipelines;

public sealed class SpscPipeOptions
{
    public MemoryPool<byte> Pool { get; }
    public int  MinimumSegmentSize    { get; }
    public long PauseWriterThreshold  { get; }
    public long ResumeWriterThreshold { get; }
    public int  MaxFreelistSegments   { get; }

    /// <summary>
    /// Routes parked-awaiter continuations to a thread of the dispatcher's choosing.
    /// When null (the default), <see cref="ThreadPoolContinuationDispatcher.Instance"/> is used,
    /// which forwards to <see cref="System.Threading.ThreadPool.UnsafeQueueUserWorkItem(Action{object?}, object?, bool)"/>
    /// — observably identical to the prior <c>RunContinuationsAsynchronously = true</c> behavior.
    /// Init-only: chosen once at pipe construction. See <see cref="IContinuationDispatcher"/> for the contract.
    /// </summary>
    public IContinuationDispatcher? ContinuationDispatcher { get; init; }

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
