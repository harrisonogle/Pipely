using System.Buffers;
using System.IO.Pipelines;

namespace SpscPipe;

// §3 Options — validated by the SpscPipe constructor, not here.
//
// Invariants (validated at construction):
//   * MinimumSegmentSize ≥ 1
//   * 0 ≤ ResumeWriterThreshold ≤ PauseWriterThreshold
public sealed class SpscPipeOptions
{
    public int  MinimumSegmentSize      { get; init; } = 4096;
    public long PauseWriterThreshold    { get; init; } = 65536;
    public long ResumeWriterThreshold   { get; init; } = 32768;

    public MemoryPool<byte>? Pool              { get; init; } = null;
    public PipeScheduler?    ReaderScheduler   { get; init; } = PipeScheduler.ThreadPool;
    public PipeScheduler?    WriterScheduler   { get; init; } = PipeScheduler.ThreadPool;
    public bool              UseSynchronizationContext { get; init; } = true;
}
