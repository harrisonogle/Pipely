using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;

namespace SpscPipe.Internal;

// §4.2 Shared-state layout.
//
// The CLR aligns heap objects to 8 bytes, not to the 64-byte cache line.
// A naïve 192-byte layout that merely pads groups to 64 bytes fails
// whenever the struct base address satisfies A mod 64 ∈ {8, …, 56},
// because a 64-byte group's used bytes can straddle a cache-line
// boundary, and the next group's used bytes can land in the same
// straddled line.  The mitigation is 128-byte spacing between groups:
// even if group N's used bytes straddle lines k and k+1, the 64 bytes
// of pad before group N+1 push it onto lines k+2 and k+3.  No two
// groups can share a cache line regardless of A mod 64.
//
// The cost is 128 extra bytes over a naïve layout — worth it for a
// per-pipe allocation.  The true-64-byte-alignment alternative (v2
// option, pinned byte[] projected via Unsafe.As) is not worth the
// ergonomic hit for v1.
[StructLayout(LayoutKind.Explicit, Size = 512)]
internal struct State
{
    // ---- Leading pad (0-63): shields Group 0 from adjacent heap allocations ----

    // ---- Group 0 (64-191): writer-published fields ----
    // Written only by the writer; read by the reader via acquire-load.

    [FieldOffset(64)]  internal Segment? Tail;
    [FieldOffset(72)]  internal long     BytesWrittenPublished;
    [FieldOffset(80)]  internal int      WriterCompletionState;        // 0 active, 2 completed
    [FieldOffset(88)]  internal ExceptionDispatchInfo? WriterException;
    [FieldOffset(96)]  internal Segment? Head;                          // set once on first splice

    // (104-191 are pad within Group 0)

    // ---- Group 1 (192-319): reader-published fields ----
    // Written only by the reader; read by the writer via acquire-load.

    [FieldOffset(192)] internal long     BytesReadPublished;
    [FieldOffset(200)] internal int      ReaderCompletionState;
    [FieldOffset(208)] internal ExceptionDispatchInfo? ReaderException;

    // (216-319 are pad within Group 1)

    // ---- Group 2 (320-447): awaiter coordination ----
    // Both sides read and write via Interlocked; double-check protocol (§8.3).

    [FieldOffset(320)] internal int      ReaderAwaiterState;
    [FieldOffset(328)] internal int      WriterAwaiterState;

    // (332-447 are pad within Group 2)

    // ---- Trailing pad (448-511): shields Group 2 from adjacent heap allocations ----
}
