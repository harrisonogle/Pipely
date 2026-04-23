using System.Buffers;

namespace SpscPipe.Internal;

// §4.1 Segment — a node in the published/unpublished singly-linked list.
//
// Inherits from ReadOnlySequenceSegment<byte> so that ReadOnlySequence<byte>
// enumerators (the external consumer's view) can walk from a start segment
// to an end segment via the base class Next property.
//
// Internal traversal (§7.2) reads the successor via _next with
// Volatile.Read/Volatile.Write for release/acquire semantics.  base.Next
// is kept in sync so external consumers see the same linkage.  We can't
// Volatile.Write directly to the base property because it has no
// addressable backing field we can pass by ref.
internal sealed class Segment : ReadOnlySequenceSegment<byte>
{
    // Shared buffer container; null after retirement.
    internal BufferHolder? Holder;

    // Offset within Holder.Owner.Memory where this segment's bytes start.
    internal int BufferStart;

    // Bytes written into this segment.  Plain int; set during
    // AppendActiveSegmentToUnpublished before publication.
    internal int WrittenLength;

    // Internal Next pointer.  base.Next is kept in sync for external
    // ReadOnlySequence<byte> consumers; _next carries the release/acquire
    // semantics SpscPipe needs internally.
    internal Segment? _next;

    // Fills all fields at rental time (§6.4.1 step 1).  base.Next's
    // protected setter forces this to live on Segment rather than in the
    // writer.  Plain stores — no release needed until the splice publishes
    // the segment via its own release-store.
    internal void Initialize(BufferHolder holder, int bufferStart, int writtenLength, long runningIndex)
    {
        Holder = holder;
        BufferStart = bufferStart;
        WrittenLength = writtenLength;
        Memory = holder.Owner!.Memory.Slice(bufferStart, writtenLength);
        RunningIndex = runningIndex;
        _next = null;
        base.Next = null;
    }

    // §7.2 acquire-load: paired with SetNextRelease on the writer side.
    // Release/acquire happens-before edge guarantees the caller observes
    // every writer write that preceded the release-store of _next.
    internal Segment? AcquireNext() => Volatile.Read(ref _next);

    // §6.4.2 release-store #1 (subsequent-splice case).  Sets base.Next
    // first (atomic reference store, for ROSeq<byte> enumerators) then
    // release-stores _next (what reader-side acquire-loads observe).
    internal void SetNextRelease(Segment? next)
    {
        base.Next = next;
        Volatile.Write(ref _next, next);
    }

    // §6.4.1 step 2: plain intra-chain Next write.  Used while the chain
    // is still writer-local; the reader cannot reach these segments until
    // the splice's release-store establishes the happens-before edge.
    internal void SetNextPlain(Segment? next)
    {
        base.Next = next;
        _next = next;
    }

    // Resets all fields for pool return.  Called from SegmentPool.Return.
    internal void Reset()
    {
        Holder = null;
        BufferStart = 0;
        WrittenLength = 0;
        _next = null;
        Memory = default;
        RunningIndex = 0;
        base.Next = null;
    }
}
