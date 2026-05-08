using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Pipely;

// Cache-line discipline note. `[StructLayout(LayoutKind.Sequential)]` is a marshaling
// hint, not a managed-layout guarantee — verified empirically on .NET 10 / x86-64,
// the CLR reorders fields for GC-bitmap efficiency on both classes with mixed fields
// AND structs containing reference fields, regardless of the attribute. The
// `WriterFields` / `ReaderFields` structs below therefore can't rely on declaration
// order for pad-before / pad-after positioning. What they *do* guarantee is total
// size: each carries 256 bytes of padding (two `CacheLinePad` fields) plus the bulk
// of its hot-path fields, so whichever way the runtime arranges fields internally,
// the writer hot region and reader hot region are forced ≥128 bytes apart in the
// containing object — enough to guarantee disjoint cache lines under any heap
// alignment. The Sequential attribute is retained as documentation of intent and on
// the off chance a future runtime honors it; the isolation does not depend on it.
public sealed partial class Pipe : IDisposable
{
    [InlineArray(128)]
    private struct CacheLinePad { private byte _b; }

    // Writer-side cursors (writer thread only). The struct's pad-before / pad-after
    // isolate the contained fields from any neighboring class field on either side,
    // independent of where the runtime places `_writer` within `Pipe`'s layout.
    [StructLayout(LayoutKind.Sequential)]
    internal struct WriterFields
    {
#pragma warning disable CS0169
        private CacheLinePad _padBefore;
#pragma warning restore CS0169

        public BufferSegment? ChainHead;
        public BufferSegment? WritingHead;
        public int  WritingHeadBytesBuffered;
        public long TotalWritten;
        public BufferSegment? FreelistHead;
        public int  FreelistCount;
        public BufferSegment? DonatedShellFreelistHead;
        public int  DonatedShellFreelistCount;
        public WriterState LastPublishedWriterState;
        public ReaderState LastAcquiredReaderState;
        public bool WriterCompleted;

#pragma warning disable CS0169
        private CacheLinePad _padAfter;
#pragma warning restore CS0169
    }

    // Reader-side cursors (reader thread only). Same discipline as WriterFields.
    [StructLayout(LayoutKind.Sequential)]
    internal struct ReaderFields
    {
#pragma warning disable CS0169
        private CacheLinePad _padBefore;
#pragma warning restore CS0169

        public BufferSegment? ReadHead;
        public int ReadHeadIdx;
        public BufferSegment? ReadTail;
        public int ReadTailIdx;
        public long TotalConsumed;
        public long TotalExamined;
        public ReaderState LastPublishedReaderState;
        public WriterState LastAcquiredWriterState;
        public bool ReaderCompleted;
        // True when a ReadResult has been delivered to the user but not yet AdvanceTo'd.
        // Set on every ReadResult-delivery site (sync return + park SetResult); cleared in AdvanceTo.
        // Cross-thread sets (signaler, canceler) ride on _core's SetResult/await synchronization edge.
        public bool ReadPending;

#pragma warning disable CS0169
        private CacheLinePad _padAfter;
#pragma warning restore CS0169
    }

    internal readonly System.IO.Pipelines.PipeOptions _options;
    internal readonly int _maxFreelistSegments;
    internal readonly TripleBuffer<WriterState> _writerTb = new();
    internal readonly TripleBuffer<ReaderState> _readerTb = new();
    internal readonly PipelyAwaiter<ReadResult>  _readAwaiter;
    internal readonly PipelyAwaiter<FlushResult> _flushAwaiter;

    internal WriterFields _writer;
    internal ReaderFields _reader;

    // Pipe-level (mutated by Dispose only).
    internal bool _disposed;

    private readonly PipeWriter _writerInstance;
    private readonly PipeReader _readerInstance;

    public Pipe() : this(PipeOptions.Default) { }

    public Pipe(PipeOptions options) : this(options, options.MaxFreelistSegments) { }

    public Pipe(System.IO.Pipelines.PipeOptions options, int? maxFreelistSegments = null)
        : this(
            options,
            maxFreelistSegments
                ?? (options as PipeOptions)?.MaxFreelistSegments
                ?? PipeOptions.Default.MaxFreelistSegments)
    { }

    private Pipe(System.IO.Pipelines.PipeOptions options, int maxFreelistSegments)
    {
        if (maxFreelistSegments < 0) throw new ArgumentOutOfRangeException(nameof(maxFreelistSegments));
        _options = options;
        _maxFreelistSegments = maxFreelistSegments;
        _readAwaiter    = new PipelyAwaiter<ReadResult>(options.ReaderScheduler, options.UseSynchronizationContext);
        _flushAwaiter   = new PipelyAwaiter<FlushResult>(options.WriterScheduler, options.UseSynchronizationContext);
        _writerInstance = new PipeWriter(this);
        _readerInstance = new PipeReader(this);
    }

    public PipeWriter Writer => _writerInstance;
    public PipeReader Reader => _readerInstance;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // R4-1: dispose leftover CTRs (idempotent on default).
        _readAwaiter._ctr.Dispose();
        _flushAwaiter._ctr.Dispose();

        // Walk the chain.
        var seg = _writer.ChainHead;
        while (seg != null)
        {
            var next = seg.Next;
            seg.DisposeOwned();
            seg = next;
        }
        _writer.ChainHead = null;
        _writer.WritingHead = null;

        // Walk the freelist.
        var fl = _writer.FreelistHead;
        while (fl != null)
        {
            var next = fl.Next;
            fl.DisposeOwned();
            fl = next;
        }
        _writer.FreelistHead = null;
        _writer.FreelistCount = 0;

        // Donated-shell freelist: shells have no IMemoryOwner (released in RecycleDrainedSegments
        // before pooling). Just clear the head and count; nothing to dispose.
        _writer.DonatedShellFreelistHead = null;
        _writer.DonatedShellFreelistCount = 0;
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
        var head = _writer.FreelistHead;
        if (head == null) return null;
        if (head.AvailableMemory.Length < minSize)
        {
            // Drop and dispose; per Spec §3 N5 (avoid stranding small segments).
            _writer.FreelistHead = head.Next;
            head.DisposeOwned();
            _writer.FreelistCount--;
            return null;
        }
        _writer.FreelistHead = head.Next;
        _writer.FreelistCount--;
        // Don't RecycleReset here — RentSegment does it with the correct runningIndex,
        // which also clears the freelist-link Next set by PushFreelist.
        return head;
    }

    internal void PushFreelist(BufferSegment s)
    {
        if (_writer.FreelistCount >= _maxFreelistSegments)
        {
            s.DisposeOwned();
            return;
        }
        // Reset to clean state, then link into the freelist via SetFreelistNext.
        s.RecycleReset(runningIndex: 0);
        s.SetFreelistNext(_writer.FreelistHead);
        _writer.FreelistHead = s;
        _writer.FreelistCount++;
    }

    internal void PushDonatedShellFreelist(BufferSegment shell)
    {
        if (_writer.DonatedShellFreelistCount >= _maxFreelistSegments)
            return;     // cap exceeded; drop the shell to GC
        shell.SetFreelistNext(_writer.DonatedShellFreelistHead);
        _writer.DonatedShellFreelistHead = shell;
        _writer.DonatedShellFreelistCount++;
    }

    internal BufferSegment? PopDonatedShellFreelist()
    {
        var head = _writer.DonatedShellFreelistHead;
        if (head == null) return null;
        _writer.DonatedShellFreelistHead = head.Next;
        head.SetFreelistNext(null);   // detach from the freelist link
        _writer.DonatedShellFreelistCount--;
        return head;
    }

    internal bool HasReadableProgress() => _reader.LastAcquiredWriterState.TotalWritten > _reader.TotalExamined;

    internal void IntegrateAcquiredWriterState()
    {
        var w = _reader.LastAcquiredWriterState;
        if (_reader.ReadHead == null)                  // I10 bootstrap
        {
            _reader.ReadHead    = w.HeadSegment;
            _reader.ReadHeadIdx = 0;
        }
        _reader.ReadTail    = w.TailSegment;
        _reader.ReadTailIdx = w.TailWritten;
    }

    internal ReadResult BuildReadResult(bool isCanceled)
    {
        bool isCompleted = _reader.LastAcquiredWriterState.IsCompleted;
        var buffer = _reader.ReadHead == null
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(_reader.ReadHead, _reader.ReadHeadIdx, _reader.ReadTail!, _reader.ReadTailIdx);
        return new ReadResult(buffer, isCanceled, isCompleted);
    }

    internal FlushResult BuildFlushResult(bool isCanceled)
        => new(isCanceled, isCompleted: _writer.LastAcquiredReaderState.IsCompleted);

    internal void SignalReadAwaiterIfPending()
    {
        while (true)
        {
            int oldV = _readAwaiter._state;
            if ((oldV & PipelyAwaiter<ReadResult>.StateMask) != PipelyAwaiter<ReadResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<ReadResult>.StateMask;
            if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _readAwaiter._signalWonCount);
                _readAwaiter._ctr.Dispose();

                // Pattern 2: construct ReadResult from stash + just-published WriterState.
                var w = _writer.LastPublishedWriterState;

                // Throw-first: writer-completed-with-ex delivered as exception.
                if (w.IsCompleted && w.CompletionException != null)
                {
                    _readAwaiter._core.SetException(w.CompletionException);
                    return;
                }

                var head    = _readAwaiter._stashHead ?? w.HeadSegment;     // bootstrap fallback
                var headIdx = _readAwaiter._stashHead == null ? 0 : _readAwaiter._stashHeadIdx;

                var buffer = head == null
                    ? ReadOnlySequence<byte>.Empty
                    : new ReadOnlySequence<byte>(head, headIdx, w.TailSegment!, w.TailWritten);

                _reader.ReadPending = true;
                _readAwaiter._core.SetResult(new ReadResult(buffer, isCanceled: false, isCompleted: w.IsCompleted));
                return;
            }
        }
    }

    internal void OnReadAwaiterTokenCancel()
    {
        while (true)
        {
            int oldV = _readAwaiter._state;
            if ((oldV & PipelyAwaiter<ReadResult>.StateMask) != PipelyAwaiter<ReadResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<ReadResult>.StateMask;
            if (Interlocked.CompareExchange(ref _readAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _readAwaiter._tokenCancelWonCount);
                _readAwaiter._core.SetException(new OperationCanceledException(_readAwaiter._token));
                return;
            }
        }
    }

    internal void OnFlushAwaiterTokenCancel()
    {
        while (true)
        {
            int oldV = _flushAwaiter._state;
            if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
            if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _flushAwaiter._tokenCancelWonCount);
                _flushAwaiter._core.SetException(new OperationCanceledException(_flushAwaiter._token));
                return;
            }
        }
    }

    internal void PublishReaderState()
    {
        var snapshot = new ReaderState
        {
            HeadSegment         = _reader.ReadHead,
            TotalConsumed       = _reader.TotalConsumed,
            TotalExamined       = _reader.TotalExamined,
            IsCompleted         = false,
            CompletionException = null,
        };
        _readerTb.ProducerSlot() = snapshot;
        _readerTb.Publish();
        _reader.LastPublishedReaderState = snapshot;

        SignalFlushIfBackpressureRelieved();
    }

    // R2-1: gated signaler — only wakes the parked writer when backpressure has relieved.
    // Called from AdvanceTo. The writer cannot re-check the wake condition after _core.SetResult,
    // so signal-side gating is required (not optional).
    //
    // Side effect: the inner TryAcquire mutates _reader.ReadTail/_reader.ReadTailIdx via IntegrateAcquiredWriterState.
    // Benign — keeps reader's view of the writer's tail fresh as a no-op-or-better.
    internal void SignalFlushIfBackpressureRelieved()
    {
        // Fast path: no parked writer.
        if ((_flushAwaiter._state & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) return;

        // Refresh writer state to compute unconsumed accurately.
        if (_writerTb.TryAcquire())
        {
            _reader.LastAcquiredWriterState = _writerTb.ConsumerSlot();
            IntegrateAcquiredWriterState();
        }

        long unconsumed = _reader.LastAcquiredWriterState.TotalWritten - _reader.TotalConsumed;
        // Note: no `|| _reader.ReaderCompleted` clause — AdvanceTo's entry guard throws if _reader.ReaderCompleted,
        // so this code path never runs post-completion. Reader.Complete uses SignalFlushAwaiterIfPending (unconditional).
        if (unconsumed >= _options.ResumeWriterThreshold) return;

        while (true)
        {
            int oldV = _flushAwaiter._state;
            if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
            if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _flushAwaiter._signalWonCount);
                _flushAwaiter._ctr.Dispose();
                DeliverFlushResult();
                return;
            }
        }
    }

    // Unconditional signaler — used by Reader.Complete only (completion is always a wake reason).
    internal void SignalFlushAwaiterIfPending()
    {
        while (true)
        {
            int oldV = _flushAwaiter._state;
            if ((oldV & PipelyAwaiter<FlushResult>.StateMask) != PipelyAwaiter<FlushResult>.Pending) return;
            int desired = oldV & ~PipelyAwaiter<FlushResult>.StateMask;
            if (Interlocked.CompareExchange(ref _flushAwaiter._state, desired, oldV) == oldV)
            {
                Interlocked.Increment(ref _flushAwaiter._signalWonCount);
                _flushAwaiter._ctr.Dispose();
                DeliverFlushResult();
                return;
            }
        }
    }

    private void DeliverFlushResult()
    {
        var r = _reader.LastPublishedReaderState;
        if (r.IsCompleted && r.CompletionException != null)
            _flushAwaiter._core.SetException(r.CompletionException);
        else
            _flushAwaiter._core.SetResult(new FlushResult(isCanceled: false, isCompleted: r.IsCompleted));
    }

    internal void RecycleDrainedSegments()
    {
        var r = _writer.LastAcquiredReaderState;
        if (r.HeadSegment is null && !r.IsCompleted) return;       // pre-bootstrap

        var readerHead = r.HeadSegment;

        while (_writer.ChainHead != _writer.WritingHead && _writer.ChainHead != readerHead)
        {
            var recycled = _writer.ChainHead!;
            _writer.ChainHead   = recycled.Next!;

            if (recycled.IsDonated)
            {
                recycled.DisposeOwned();              // foreign owner: release the IMemoryOwner
                PushDonatedShellFreelist(recycled);   // shell pooled for re-use; over-cap drops to GC
            }
            else
            {
                PushFreelist(recycled);               // pool-rented: existing freelist path (with cap-overflow handling)
            }
        }
    }
}
