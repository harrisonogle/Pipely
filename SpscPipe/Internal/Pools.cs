using System.Collections.Concurrent;

namespace SpscPipe.Internal;

// §9 Pools — bounded concurrent object pools for Segment and BufferHolder.
//
// Per-pipe instances; sized by SpscPipeOptions (§9 default:
// 2 × PauseWriterThreshold / MinimumSegmentSize).  Both sides contend
// (writer rents during publication; reader returns during retirement),
// so the pool's own synchronization applies.  Backed by ConcurrentQueue
// for the v1; swap for a partitioned SPSC pool if profiling demands.

internal sealed class SegmentPool
{
    private readonly ConcurrentQueue<Segment> _pool = new();
    private readonly int _maxSize;
    private int _currentSize;

    internal SegmentPool(int maxSize) => _maxSize = maxSize;

    internal Segment Rent()
    {
        if (_pool.TryDequeue(out var seg))
        {
            Interlocked.Decrement(ref _currentSize);
            return seg;
        }
        return new Segment();
    }

    internal void Return(Segment seg)
    {
        seg.Reset();
        if (Interlocked.Increment(ref _currentSize) <= _maxSize)
        {
            _pool.Enqueue(seg);
        }
        else
        {
            Interlocked.Decrement(ref _currentSize);
            // Drop on the floor; let GC reclaim.
        }
    }
}

internal sealed class BufferHolderPool
{
    private readonly ConcurrentQueue<BufferHolder> _pool = new();
    private readonly int _maxSize;
    private int _currentSize;

    internal BufferHolderPool(int maxSize) => _maxSize = maxSize;

    internal BufferHolder Rent()
    {
        if (_pool.TryDequeue(out var holder))
        {
            Interlocked.Decrement(ref _currentSize);
            return holder;
        }
        return new BufferHolder();
    }

    internal void Return(BufferHolder holder)
    {
        holder.Owner = null;
        holder.Refcount = 0;
        if (Interlocked.Increment(ref _currentSize) <= _maxSize)
        {
            _pool.Enqueue(holder);
        }
        else
        {
            Interlocked.Decrement(ref _currentSize);
        }
    }
}
