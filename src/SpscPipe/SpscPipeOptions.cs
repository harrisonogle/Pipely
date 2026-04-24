using System.Buffers;
using System.IO.Pipelines;

namespace SpscPipe;

// Per spec §3.
//
// Validation + coercion mirror System.IO.Pipelines.PipeOptions:
//
//   - MinimumSegmentSize must be >= 1.
//   - PauseWriterThreshold < 0 throws.
//     PauseWriterThreshold == 0 disables backpressure entirely (unlimited
//     pipe — FlushAsync never parks, §6.3 step 5 early return).
//   - ResumeWriterThreshold < 0 throws.
//     ResumeWriterThreshold == 0 is coerced to 1: zero would leave a
//     paused writer un-resumable because the §8.4/§8.5 signal condition
//     `outstanding < Resume` is unreachable (outstanding is always >= 0).
//     BCL's PipeOptions does the same coercion.
//   - When PauseWriterThreshold > 0, ResumeWriterThreshold must be
//     <= PauseWriterThreshold.
//   - When PauseWriterThreshold == 0, the Resume <= Pause constraint is
//     not enforced (Resume is irrelevant under an unlimited pipe).
//
// The `-1` sentinel BCL uses for "apply the default" is not supported here:
// this type uses init-only properties with defaults, so "use default" means
// "don't set the property."  A caller writing
// `new SpscPipeOptions { PauseWriterThreshold = -1 }` is explicitly asking
// for -1 and gets an exception.
public sealed class SpscPipeOptions
{
    private readonly long _resumeWriterThreshold = 32768;

    public int MinimumSegmentSize { get; init; } = 4096;

    public long PauseWriterThreshold { get; init; } = 65536;

    public long ResumeWriterThreshold
    {
        get => _resumeWriterThreshold;
        // BCL PipeOptions: "A resumeWriterThreshold of 0 makes no sense
        // because the writer could never resume if paused. By setting it
        // to 1, the writer will resume only after all data is consumed."
        init => _resumeWriterThreshold = value == 0 ? 1 : value;
    }

    public MemoryPool<byte>? Pool { get; init; } = null;
    public PipeScheduler? ReaderScheduler { get; init; } = PipeScheduler.ThreadPool;
    public PipeScheduler? WriterScheduler { get; init; } = PipeScheduler.ThreadPool;
    public bool UseSynchronizationContext { get; init; } = true;

    internal void Validate()
    {
        if (MinimumSegmentSize < 1)
            throw new ArgumentOutOfRangeException(nameof(MinimumSegmentSize));
        if (PauseWriterThreshold < 0)
            throw new ArgumentOutOfRangeException(nameof(PauseWriterThreshold));
        if (ResumeWriterThreshold < 0)
            throw new ArgumentOutOfRangeException(nameof(ResumeWriterThreshold));
        // Resume <= Pause only enforced when the pipe can actually pause
        // (PauseWriterThreshold > 0).  When PauseWriterThreshold == 0 the
        // pipe never parks, so Resume's value is irrelevant — matching BCL.
        if (PauseWriterThreshold > 0 && ResumeWriterThreshold > PauseWriterThreshold)
            throw new ArgumentOutOfRangeException(nameof(ResumeWriterThreshold));
    }
}
