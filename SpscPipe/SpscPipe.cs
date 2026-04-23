using System.Buffers;
using System.IO.Pipelines;
using SpscPipe.Internal;

namespace SpscPipe;

// §3 Public entry point.  Owns the shared State, the reader and writer
// facades, the pools, and the buffer MemoryPool<byte>.
public sealed class SpscPipe : IDisposable
{
    private static readonly SpscPipeOptions s_defaultOptions = new();

    private readonly SpscPipeOptions _options;

    // Shared state (§4.2).  Mutated by writer (Group 0), reader (Group 1),
    // and both via Interlocked (Group 2).  Struct-valued to keep all cross-
    // thread fields in a single contiguous allocation with controlled
    // cache-line layout.  Written via ref in later checkpoints; default
    // initialization here zeroes all fields per Init in the TLA+ model.
    internal State _state = default;

    // Internal so each side can reach the other's awaiter field for
    // cross-side SetResult calls (§8.2 writer-signals-reader, §8.4
    // reader-signals-writer).  Exposed via Reader / Writer properties
    // below for external consumers.
    internal readonly SpscPipeReader _reader;
    internal readonly SpscPipeWriter _writer;

    internal readonly SegmentPool       _segmentPool;
    internal readonly BufferHolderPool  _bufferHolderPool;
    internal readonly MemoryPool<byte>  _memoryPool;

    public SpscPipe() : this(s_defaultOptions) { }

    public SpscPipe(SpscPipeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.MinimumSegmentSize < 1)
            throw new ArgumentOutOfRangeException(
                nameof(options), options.MinimumSegmentSize,
                "MinimumSegmentSize must be at least 1.");

        if (options.PauseWriterThreshold < 0)
            throw new ArgumentOutOfRangeException(
                nameof(options), options.PauseWriterThreshold,
                "PauseWriterThreshold must be non-negative.");

        if (options.ResumeWriterThreshold < 0)
            throw new ArgumentOutOfRangeException(
                nameof(options), options.ResumeWriterThreshold,
                "ResumeWriterThreshold must be non-negative.");

        if (options.ResumeWriterThreshold > options.PauseWriterThreshold)
            throw new ArgumentOutOfRangeException(
                nameof(options), options.ResumeWriterThreshold,
                "ResumeWriterThreshold must not exceed PauseWriterThreshold.");

        _options = options;

        // §9 default sizing: 2 × PauseWriterThreshold / MinimumSegmentSize.
        // Clamp to a minimum of 16 so tiny-threshold configurations aren't starved.
        var poolSize = Math.Max(
            16,
            (int)(2 * options.PauseWriterThreshold / options.MinimumSegmentSize));

        _segmentPool      = new SegmentPool(poolSize);
        _bufferHolderPool = new BufferHolderPool(poolSize);
        _memoryPool       = options.Pool ?? MemoryPool<byte>.Shared;

        _reader = new SpscPipeReader(this);
        _writer = new SpscPipeWriter(this);
    }

    // §6.5.2 shared ReleaseHolder helper.  Called by the writer during
    // buffer rotation / Complete, and by the reader during segment
    // retirement.  Interlocked.Decrement is a full fence by the §5 axiom,
    // which orders all prior writes/reads on this thread globally before
    // the decrement — so if this thread drives the count to 0, no other
    // thread holds an outstanding reference to the buffer.
    internal void ReleaseHolder(BufferHolder holder)
    {
        if (Interlocked.Decrement(ref holder.Refcount) == 0)
        {
            holder.Owner!.Dispose();  // returns the buffer to MemoryPool<byte>
            _bufferHolderPool.Return(holder);
        }
    }

    public PipeReader Reader => _reader;
    public PipeWriter Writer => _writer;

    internal SpscPipeOptions Options => _options;

    // §10.5.  Requires both ends completed, with no operations in flight.
    // Performs the same cleanup walks as Dispose, then re-initializes the
    // pipe for reuse.
    public void Reset()
    {
        if (_state.WriterCompletionState != 2 || _state.ReaderCompletionState != 2)
            throw new InvalidOperationException(
                "Reset requires both the writer and reader to have completed.");

        _writer.DoCleanup();
        _reader.DoCleanup();

        _state = default;

        _writer.ReInitForReuse();
        _reader.ReInitForReuse();
    }

    // §10.6 Dispose — walks unpublished (writer-local) and published
    // chains, releasing holders and returning segments to pools.  Then
    // zeros state and suppresses the finalizer.  Not safe to call
    // concurrently with active reader or writer operations; the caller
    // must ensure no ops are in flight.
    public void Dispose()
    {
        _writer.DoCleanup();
        _reader.DoCleanup();
        _state = default;
        GC.SuppressFinalize(this);
    }

    // §10.6 Finalizer — weak safety net for abandoned pipes.  Particularly
    // important for custom MemoryPool<byte> implementations backed by
    // pinned/native memory; for ArrayPool<byte>.Shared-backed buffers the
    // GC would eventually reclaim anyway.  Reads writer/reader state
    // without explicit Volatile.Read; relies on the GC's stop-the-world
    // phase to induce the necessary memory barrier before the finalizer
    // thread runs.
    ~SpscPipe()
    {
        try
        {
            _writer.DoCleanup();
            _reader.DoCleanup();
        }
        catch
        {
            // Finalizers must not throw.
        }
    }
}
