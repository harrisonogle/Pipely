using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;

namespace SpscPipe;

// Per spec §4.2.  Shared state with explicit cache-line spacing.
// Pointer alignment on the heap is only 8 bytes, so groups are spaced
// by 128 bytes to guarantee no two groups share a cache line
// irrespective of the struct base address (see §4.2 rationale).
[StructLayout(LayoutKind.Explicit, Size = 512)]
internal struct State
{
    // Group 0 (offsets 64..191): writer-published fields.
    [FieldOffset(64)]  internal Segment? Tail;
    [FieldOffset(72)]  internal long BytesWrittenPublished;
    [FieldOffset(80)]  internal int WriterCompletionState;    // 0 = active, 2 = completed
    [FieldOffset(88)]  internal ExceptionDispatchInfo? WriterException;
    [FieldOffset(96)]  internal Segment? Head;

    // Group 1 (offsets 192..319): reader-published fields.
    [FieldOffset(192)] internal long BytesReadPublished;
    [FieldOffset(200)] internal int ReaderCompletionState;
    [FieldOffset(208)] internal ExceptionDispatchInfo? ReaderException;

    // Group 2 (offsets 320..447): awaiter coordination.
    [FieldOffset(320)] internal int ReaderAwaiterState;       // Idle=0, Armed=1, Signaled=2
    [FieldOffset(328)] internal int WriterAwaiterState;       // Idle=0, Armed=1, Signaled=2

    // Leading/trailing pads (0..63 and 448..511) shield adjacent allocations.
}

internal static class AwaiterState
{
    internal const int Idle     = 0;
    internal const int Armed    = 1;
    internal const int Signaled = 2;
}

internal static class CompletionState
{
    internal const int Active    = 0;
    internal const int Completed = 2;
}
