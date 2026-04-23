namespace SpscPipe.Internal;

// §8.1 Awaiter state machine values.  Each awaiter (one per side) is an
// int on the shared State; transitions happen via Interlocked.CompareExchange.
// The waiter transitions Idle → Armed → Idle; the signaler transitions
// Armed → Signaled.  The intermediate "arming" phase (decide to wait, then
// re-check before committing to park) is procedural per §8.3 steps A–C,
// not a discrete state value.
internal static class AwaiterStates
{
    public const int Idle     = 0;
    public const int Armed    = 1;
    public const int Signaled = 2;
}

// §4.4 ReadSignal — minimal payload carried by the reader's internal
// ManualResetValueTaskSourceCore.  The writer (signaler) cannot build a
// ReadResult because ReadResult depends on reader-local state.  Instead
// the writer signals with this flag; SpscPipeReader implements
// IValueTaskSource<ReadResult> and translates the signal into a real
// ReadResult on the reader's scheduled continuation thread (§8.7).
internal readonly struct ReadSignal
{
    internal bool IsCanceled { get; init; }
}
