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

    private readonly SpscPipeReader _reader;
    private readonly SpscPipeWriter _writer;

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

    // §10.5 Reset and §10.6 Dispose are implemented in checkpoint 4.
    public void Reset()   => throw new NotImplementedException();
    public void Dispose() => throw new NotImplementedException();
}
