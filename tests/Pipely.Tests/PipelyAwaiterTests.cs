using System.Threading;
using System.Threading.Tasks.Sources;
using Xunit;

namespace PipelyTests;

public class PipelyAwaiterTests
{
    [Fact]
    public void NewAwaiter_StateIsInactive()
    {
        var a = new Pipely.PipelyAwaiter<int>(Pipely.ThreadPoolContinuationDispatcher.Instance);
        Assert.Equal(Pipely.PipelyAwaiter<int>.Inactive, a._state);
    }

    [Fact]
    public void Or_CancelFlag_SetsBitAndReturnsOldValue()
    {
        var a = new Pipely.PipelyAwaiter<int>(Pipely.ThreadPoolContinuationDispatcher.Instance);
        int old = Interlocked.Or(ref a._state, Pipely.PipelyAwaiter<int>.CancelFlag);

        Assert.Equal(Pipely.PipelyAwaiter<int>.Inactive, old);
        Assert.Equal(Pipely.PipelyAwaiter<int>.CancelFlag, a._state);
    }

    [Fact]
    public void OwnerCanClearCancelFlagViaCAS()
    {
        var a = new Pipely.PipelyAwaiter<int>(Pipely.ThreadPoolContinuationDispatcher.Instance);
        a._state = Pipely.PipelyAwaiter<int>.CancelFlag;     // simulate canceler's Or

        int prior = Interlocked.CompareExchange(ref a._state, Pipely.PipelyAwaiter<int>.Inactive, Pipely.PipelyAwaiter<int>.CancelFlag);

        Assert.Equal(Pipely.PipelyAwaiter<int>.CancelFlag, prior);
        Assert.Equal(Pipely.PipelyAwaiter<int>.Inactive, a._state);
    }

    [Fact]
    public async Task ParkThenSignal_DeliversResult()
    {
        var a = new Pipely.PipelyAwaiter<int>(Pipely.ThreadPoolContinuationDispatcher.Instance);
        a._core.Reset();

        // Owner: CAS Inactive → Pending.
        int prior = Interlocked.CompareExchange(ref a._state, Pipely.PipelyAwaiter<int>.Pending, Pipely.PipelyAwaiter<int>.Inactive);
        Assert.Equal(Pipely.PipelyAwaiter<int>.Inactive, prior);

        var task = new ValueTask<int>(a, a.Version);

        // Signaler: CAS Pending → Inactive (state cleared, flag preserved). Then SetResult.
        int oldV = a._state;
        int desired = oldV & ~Pipely.PipelyAwaiter<int>.StateMask;
        int seen = Interlocked.CompareExchange(ref a._state, desired, oldV);
        Assert.Equal(Pipely.PipelyAwaiter<int>.Pending, seen);

        a._core.SetResult(42);

        Assert.Equal(42, await task);
    }

    [Fact]
    public async Task ParkThenCancelerSetsFlagThenSignaler_FlagPreservedAfterDelivery()
    {
        var a = new Pipely.PipelyAwaiter<int>(Pipely.ThreadPoolContinuationDispatcher.Instance);
        a._core.Reset();

        Interlocked.CompareExchange(ref a._state, Pipely.PipelyAwaiter<int>.Pending, Pipely.PipelyAwaiter<int>.Inactive);

        // Canceler races first: Or flag.
        Interlocked.Or(ref a._state, Pipely.PipelyAwaiter<int>.CancelFlag);
        Assert.Equal(Pipely.PipelyAwaiter<int>.Pending | Pipely.PipelyAwaiter<int>.CancelFlag, a._state);

        var task = new ValueTask<int>(a, a.Version);

        // Signaler wins CAS Pending|Flag → Inactive|Flag (state cleared, flag preserved).
        int oldV = a._state;
        int desired = oldV & ~Pipely.PipelyAwaiter<int>.StateMask;
        int seen = Interlocked.CompareExchange(ref a._state, desired, oldV);
        Assert.Equal(Pipely.PipelyAwaiter<int>.Pending | Pipely.PipelyAwaiter<int>.CancelFlag, seen);
        a._core.SetResult(7);

        Assert.Equal(7, await task);
        // Flag remains set; next Park's R4 lost-cancel re-check picks it up.
        Assert.Equal(Pipely.PipelyAwaiter<int>.CancelFlag, a._state);
    }

    [Fact]
    public async Task CancelerWinsCAS_DeliversResultAndClearsFlag()
    {
        var a = new Pipely.PipelyAwaiter<int>(Pipely.ThreadPoolContinuationDispatcher.Instance);
        a._core.Reset();

        Interlocked.CompareExchange(ref a._state, Pipely.PipelyAwaiter<int>.Pending, Pipely.PipelyAwaiter<int>.Inactive);
        Interlocked.Or(ref a._state, Pipely.PipelyAwaiter<int>.CancelFlag);

        var task = new ValueTask<int>(a, a.Version);

        // Canceler CAS Pending|Flag → Inactive (flag cleared by this CAS).
        int seen = Interlocked.CompareExchange(
            ref a._state,
            Pipely.PipelyAwaiter<int>.Inactive,
            Pipely.PipelyAwaiter<int>.Pending | Pipely.PipelyAwaiter<int>.CancelFlag);
        Assert.Equal(Pipely.PipelyAwaiter<int>.Pending | Pipely.PipelyAwaiter<int>.CancelFlag, seen);
        a._core.SetResult(99);

        Assert.Equal(99, await task);
        Assert.Equal(Pipely.PipelyAwaiter<int>.Inactive, a._state);
    }

    [Fact]
    public void StashFields_AreReadableAfterAssignment()
    {
        var a = new Pipely.PipelyAwaiter<int>(Pipely.ThreadPoolContinuationDispatcher.Instance);
        var head = new Pipely.BufferSegment();
        head.RentFrom(System.Buffers.MemoryPool<byte>.Shared, 1024, 0, this);
        var tail = new Pipely.BufferSegment();
        tail.RentFrom(System.Buffers.MemoryPool<byte>.Shared, 1024, 1024, this);

        a._stashHead    = head;
        a._stashHeadIdx = 100;
        a._stashTail    = tail;
        a._stashTailIdx = 200;

        Assert.Same(head, a._stashHead);
        Assert.Equal(100, a._stashHeadIdx);
        Assert.Same(tail, a._stashTail);
        Assert.Equal(200, a._stashTailIdx);
    }
}
