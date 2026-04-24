using System.Buffers;

namespace SpscPipe;

// Per spec §4.1.  Mutable pooled segment.  Memory is set at initialize
// time; Next writes use Volatile.Write for release semantics (§7.2).
internal sealed class Segment : ReadOnlySequenceSegment<byte>
{
    internal BufferHolder? Holder;
    internal int BufferStart;
    internal int WrittenLength;

    // Exposes the base-class property as a typed getter for convenience.
    internal Segment? TypedNext => (Segment?)Next;

    // Reset the segment for return to the pool.  Does NOT clear Holder —
    // Holder is cleared by the retirement path after ReleaseHolder
    // (§6.5.2).
    internal void Reset()
    {
        BufferStart = 0;
        WrittenLength = 0;
        Memory = default;
        RunningIndex = 0;
        Next = null;
    }
}
