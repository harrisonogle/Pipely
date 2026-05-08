using System.IO.Pipelines;
using Pipely;
using Xunit;

namespace PipelyTests;

public class PipelyAwaiterResetTests
{
    [Fact]
    public void Reset_ClearsExternallyVisibleFields()
    {
        // Note on coverage: the private fields on PipelyAwaiter (_realContinuation, _realState,
        // _capturedEC, _capturedSC, _runCb, _runState) cannot be exercised from outside the type
        // without a test-only helper. The Reset() body also clears them, but that is verified by
        // code inspection rather than this test. The Pipe-level integration tests in
        // PipeResetTests cover the live OnCompleted → SetResult → Reset path.
        var awaiter = new PipelyAwaiter<int>(PipeScheduler.Inline, useSynchronizationContext: false);

        // Dirty every resettable field reachable from outside the type.
        awaiter._state = PipelyAwaiter<int>.Pending | PipelyAwaiter<int>.CancelFlag;
        awaiter._token = new System.Threading.CancellationToken(canceled: true);
        awaiter._stashHead = null;       // BufferSegment is internal; null is fine — we only check it's still null after Reset.
        awaiter._stashHeadIdx = 17;
        awaiter._stashTailIdx = 99;
        awaiter._parkCount = 5;
        awaiter._signalWonCount = 4;
        awaiter._tokenCancelWonCount = 3;
        awaiter._cancelPendingWonCount = 2;
        awaiter._lostWakeupResolvedCount = 1;
        awaiter._lostCancelResolvedCount = 7;

        awaiter.Reset();

        Assert.Equal(0, awaiter._state);
        Assert.Equal(default, awaiter._token);
        Assert.Null(awaiter._stashHead);
        Assert.Equal(0, awaiter._stashHeadIdx);
        Assert.Null(awaiter._stashTail);
        Assert.Equal(0, awaiter._stashTailIdx);
        Assert.Equal(0, awaiter._parkCount);
        Assert.Equal(0, awaiter._signalWonCount);
        Assert.Equal(0, awaiter._tokenCancelWonCount);
        Assert.Equal(0, awaiter._cancelPendingWonCount);
        Assert.Equal(0, awaiter._lostWakeupResolvedCount);
        Assert.Equal(0, awaiter._lostCancelResolvedCount);
    }

    [Fact]
    public void Reset_BumpsCoreVersion_InvalidatingStaleTokens()
    {
        // MRVTSC.Reset increments the version; any stale ValueTask token is now invalid.
        // We can't easily synthesize a stale token without going through OnCompleted, but we
        // can verify the version moves forward across Reset.
        var awaiter = new PipelyAwaiter<int>(PipeScheduler.Inline);
        short v0 = awaiter.Version;
        awaiter.Reset();
        Assert.NotEqual(v0, awaiter.Version);
    }
}
