using System.Buffers;
using System.IO.Pipelines;
using System.Threading;

namespace Pipely;

public sealed class PipeOptions
{
    public MemoryPool<byte> Pool { get; }
    public int  MinimumSegmentSize    { get; }
    public long PauseWriterThreshold  { get; }
    public long ResumeWriterThreshold { get; }
    public int  MaxFreelistSegments   { get; }

    /// <summary>
    /// Routes the reader's parked <c>ReadAsync</c> continuations to a thread of
    /// the scheduler's choosing. Used when the reader awaits an empty pipe and
    /// the writer signals the read awaiter on a subsequent flush. When null
    /// (the default), <see cref="PipeScheduler.ThreadPool"/> is used.
    /// Init-only: chosen once at pipe construction. See the BCL
    /// <see cref="PipeScheduler"/> contract. Mirrors
    /// <see cref="System.IO.Pipelines.PipeOptions.ReaderScheduler"/>.
    /// </summary>
    public PipeScheduler? ReaderScheduler { get; init; }

    /// <summary>
    /// Routes the writer's parked <c>FlushAsync</c> continuations to a thread of
    /// the scheduler's choosing. Used when the writer is paused at the
    /// pause-writer threshold and the reader advances past resume, signaling the
    /// flush awaiter. When null (the default), <see cref="PipeScheduler.ThreadPool"/>
    /// is used. Init-only: chosen once at pipe construction. See the BCL
    /// <see cref="PipeScheduler"/> contract. Mirrors
    /// <see cref="System.IO.Pipelines.PipeOptions.WriterScheduler"/>.
    /// </summary>
    public PipeScheduler? WriterScheduler { get; init; }

    /// <summary>
    /// When true (the default), parked read/flush continuations honor a non-default
    /// <see cref="SynchronizationContext"/> captured at the await site, dispatching the
    /// continuation via <see cref="SynchronizationContext.Post"/> instead of the configured
    /// <see cref="ReaderScheduler"/>/<see cref="WriterScheduler"/>. When false, the configured
    /// PipeScheduler always runs the continuation. The default base SynchronizationContext
    /// (i.e. one whose runtime type is exactly <see cref="SynchronizationContext"/>) is treated
    /// as "no SC" and falls through to the PipeScheduler. Init-only: chosen once at pipe
    /// construction. Mirrors <see cref="System.IO.Pipelines.PipeOptions.UseSynchronizationContext"/>.
    /// </summary>
    public bool UseSynchronizationContext { get; init; } = true;

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
