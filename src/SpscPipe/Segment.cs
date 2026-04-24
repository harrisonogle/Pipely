using System.Buffers;

namespace SpscPipe;

// Per spec §4.1.  Mutable pooled segment.
//
// Memory-ordering note.  ReadOnlySequenceSegment<byte>.Next is an auto-
// property whose backing field is not directly ref-able, so Volatile.Write
// on it is impossible through the base API.  We maintain a private _next
// field used for release/acquire in our protocol (§7.2) and keep base.Next
// in sync so the BCL's ReadOnlySequence<byte> iterator walks correctly.
//
// The reader's traversal uses Volatile.Read(ref seg._next) to acquire the
// next link (§7.2); after this the happens-before edge with the writer's
// matching Volatile.Write releases all of newSeg.*, including base.Next.
// The user's subsequent ReadOnlySequence iteration reads base.Next plain,
// which is safe because that iteration happens-after the acquire-load on
// the same thread.
internal sealed class Segment : ReadOnlySequenceSegment<byte>
{
    internal BufferHolder? Holder;
    internal int BufferStart;
    internal int WrittenLength;

    // Private backing field used for release/acquire on seg.Next per §7.2.
    private Segment? _next;

    // Release-store of Next for use by the writer's splice (§6.4.2).
    internal void SetNextRelease(Segment? next)
    {
        Volatile.Write(ref _next, next);  // §7.2 release; pairs with AcquireNext
        Next = next;                      // keep base in sync for ReadOnlySequence iterator
    }

    // Plain-store intra-chain Next (§6.4.1 step 2).  Visibility to the
    // reader is deferred to the splice's release-store which establishes
    // the happens-before edge covering this plain write.
    internal void SetNextPlain(Segment? next)
    {
        _next = next;
        Next = next;
    }

    // Acquire-load of Next per §7.2.
    internal Segment? AcquireNext()
        => Volatile.Read(ref _next);  // §7.2 acquire

    // Set base-class Memory (protected setter) from a non-derived caller.
    internal void SetMemory(ReadOnlyMemory<byte> memory) => Memory = memory;

    // Set base-class RunningIndex (protected setter).
    internal void SetRunningIndex(long index) => RunningIndex = index;

    // Reset the segment for return to the pool.  Holder is cleared by the
    // retirement path after ReleaseHolder (§6.5.2).
    internal void Reset()
    {
        BufferStart = 0;
        WrittenLength = 0;
        Memory = default;
        RunningIndex = 0;
        _next = null;
        Next = null;
    }
}
