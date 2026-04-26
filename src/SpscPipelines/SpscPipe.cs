using System.IO.Pipelines;

namespace SpscPipelines;

public sealed partial class SpscPipe : IDisposable
{
    internal readonly SpscPipeOptions _options;
    internal readonly TripleBuffer<WriterState> _writerTb = new();
    internal readonly TripleBuffer<ReaderState> _readerTb = new();
    internal readonly SpscAwaiter<ReadResult>  _readAwaiter  = new();
    internal readonly SpscAwaiter<FlushResult> _flushAwaiter = new();

    // Writer-side cursors (writer thread only).
    internal BufferSegment? _chainHead;
    internal BufferSegment? _writingHead;
    internal int  _writingHeadBytesBuffered;
    internal long _totalWritten;
    internal BufferSegment? _freelistHead;
    internal int  _freelistCount;
    internal WriterState _lastPublishedWriterState;
    internal ReaderState _lastAcquiredReaderState;
    internal bool _writerCompleted;

    // Reader-side cursors (reader thread only).
    internal BufferSegment? _readHead;
    internal int _readHeadIdx;
    internal BufferSegment? _readTail;
    internal int _readTailIdx;
    internal long _totalConsumed;
    internal long _totalExamined;
    internal ReaderState _lastPublishedReaderState;
    internal WriterState _lastAcquiredWriterState;
    internal bool _readerCompleted;

    // Pipe-level (mutated by Dispose only).
    internal bool _disposed;

    private readonly SpscPipeWriter _writerInstance;
    private readonly SpscPipeReader _readerInstance;

    public SpscPipe() : this(SpscPipeOptions.Default) { }
    public SpscPipe(SpscPipeOptions options)
    {
        _options        = options;
        _writerInstance = new SpscPipeWriter(this);
        _readerInstance = new SpscPipeReader(this);
    }

    public PipeWriter Writer => _writerInstance;
    public PipeReader Reader => _readerInstance;

    public void Dispose()
    {
        // Full implementation in Task 10.
        if (_disposed) return;
        _disposed = true;
    }

    internal BufferSegment RentSegment(int sizeHint, long runningIndex)
    {
        var s = PopFreelist(minSize: sizeHint);
        if (s != null)
        {
            s.RecycleReset(runningIndex);
            return s;
        }
        s = new BufferSegment();
        s.RentFrom(_options.Pool, Math.Max(sizeHint, _options.MinimumSegmentSize), runningIndex, owner: this);
        return s;
    }

    private BufferSegment? PopFreelist(int minSize)
    {
        var head = _freelistHead;
        if (head == null) return null;
        if (head.AvailableMemory.Length < minSize)
        {
            // Drop and dispose; per Spec §3 N5 (avoid stranding small segments).
            _freelistHead = head.Next;
            head.DisposeOwned();
            _freelistCount--;
            return null;
        }
        _freelistHead = head.Next;
        _freelistCount--;
        // Don't RecycleReset here — RentSegment does it with the correct runningIndex,
        // which also clears the freelist-link Next set by PushFreelist.
        return head;
    }

    internal void PushFreelist(BufferSegment s)
    {
        if (_freelistCount >= _options.MaxFreelistSegments)
        {
            s.DisposeOwned();
            return;
        }
        // Reset to clean state, then link into the freelist via SetFreelistNext.
        s.RecycleReset(runningIndex: 0);
        s.SetFreelistNext(_freelistHead);
        _freelistHead = s;
        _freelistCount++;
    }
}
