using Xunit;

namespace PipelyTests;

public class PipeResetTests
{
    [Fact]
    public void Reset_NeitherSideCompleted_Throws()
    {
        var pipe = new Pipely.Pipe();
        var ex = Assert.Throws<InvalidOperationException>(() => pipe.Reset());
        Assert.Equal("Both completion routines must be called before resetting the pipe.", ex.Message);
    }

    [Fact]
    public void Reset_OnlyWriterCompleted_Throws()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete();
        Assert.Throws<InvalidOperationException>(() => pipe.Reset());
    }

    [Fact]
    public void Reset_OnlyReaderCompleted_Throws()
    {
        var pipe = new Pipely.Pipe();
        pipe.Reader.Complete();
        Assert.Throws<InvalidOperationException>(() => pipe.Reset());
    }

    [Fact]
    public void Reset_BothCompleted_DoesNotThrow()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();   // must not throw
    }

    [Fact]
    public async Task Reset_ClearsAllPerInstanceFieldsToPostConstructionValues()
    {
        var pipe = new Pipely.Pipe();

        // Drive the pipe through a full round of usage.
        var mem = pipe.Writer.GetMemory(50); mem.Span.Fill(0xAB); pipe.Writer.Advance(50);
        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);
        pipe.Writer.Complete();
        pipe.Reader.Complete();

        pipe.Reset();

        // WriterFields restored.
        Assert.Null(pipe._writer.ChainHead);
        Assert.Null(pipe._writer.WritingHead);
        Assert.Equal(0, pipe._writer.WritingHeadBytesBuffered);
        Assert.Equal(0L, pipe._writer.TotalWritten);
        Assert.Equal(default, pipe._writer.LastPublishedWriterState);
        Assert.Equal(default, pipe._writer.LastAcquiredReaderState);
        Assert.False(pipe._writer.WriterCompleted);

        // ReaderFields restored.
        Assert.Null(pipe._reader.ReadHead);
        Assert.Equal(0, pipe._reader.ReadHeadIdx);
        Assert.Null(pipe._reader.ReadTail);
        Assert.Equal(0, pipe._reader.ReadTailIdx);
        Assert.Equal(0L, pipe._reader.TotalConsumed);
        Assert.Equal(0L, pipe._reader.TotalExamined);
        Assert.Equal(default, pipe._reader.LastPublishedReaderState);
        Assert.Equal(default, pipe._reader.LastAcquiredWriterState);
        Assert.False(pipe._reader.ReaderCompleted);
        Assert.False(pipe._reader.ReadPending);

        // Awaiter state.
        Assert.Equal(0, pipe._readAwaiter._state);
        Assert.Equal(0, pipe._flushAwaiter._state);
        Assert.Equal(0L, pipe._readAwaiter._parkCount);
        Assert.Equal(0L, pipe._flushAwaiter._parkCount);

        // Triple buffers: no stale terminal publication remains in any of the 3 slots.
        // (TryAcquire returns false iff the dirty bit is 0, which Reset establishes.)
        Assert.False(pipe._writerTb.TryAcquire());
        Assert.False(pipe._readerTb.TryAcquire());
    }

    [Fact]
    public async Task Reset_PreservesRentedSegmentFreelist()
    {
        // Force the pipe to recycle a rented segment to the freelist.
        // RecycleDrainedSegments only advances past a segment once the reader's
        // published HeadSegment is strictly ahead of ChainHead. That requires the
        // reader to advance to a position on seg2, which happens after the second read.
        // A third flush then runs RecycleDrainedSegments with readerHead=seg2 > ChainHead=seg1.
        var pipe = new Pipely.Pipe(new Pipely.PipeOptions(minimumSegmentSize: 64));

        // Round 1: write + read + advance past seg1.
        pipe.Writer.GetMemory(60); pipe.Writer.Advance(60);   // seg1 (64-byte slot, 60 used)
        await pipe.Writer.FlushAsync();
        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.End);                 // ReadHead = seg1

        // Round 2: write + read + advance past seg2. ReadHead moves to seg2.
        pipe.Writer.GetMemory(60); pipe.Writer.Advance(60);   // seg2 (seg1 has 4 bytes left < 60)
        await pipe.Writer.FlushAsync();
        var r2 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r2.Buffer.End);                 // ReadHead = seg2

        // Round 3: third flush triggers RecycleDrainedSegments with readerHead=seg2,
        // recycling seg1 (ChainHead) into the freelist.
        pipe.Writer.GetMemory(60); pipe.Writer.Advance(60);   // seg3
        await pipe.Writer.FlushAsync();                       // seg1 now recycled to freelist

        int freelistBefore = pipe._writer.FreelistCount;
        Assert.True(freelistBefore > 0, "Test setup error: rented freelist should be populated.");

        // Drain the chain so post-Reset only the freelist matters.
        var r3 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r3.Buffer.End);

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        // Freelist preserved across Reset (matches BCL preserving its segment pool).
        Assert.True(pipe._writer.FreelistCount >= freelistBefore,
            $"Freelist should not shrink (was {freelistBefore}, now {pipe._writer.FreelistCount}).");
    }

    [Fact]
    public async Task Reset_DonatedSegmentsInChain_DisposeOwnersAndPoolShells()
    {
        var pipe = new Pipely.Pipe();
        var donated1 = new TrackingMemoryOwner(20);
        var donated2 = new TrackingMemoryOwner(30);

        pipe.Writer.Splice(donated1);
        pipe.Writer.Splice(donated2);
        await pipe.Writer.FlushAsync();
        // Reader does NOT drain — chain is full of donated segments.

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        // Donated IMemoryOwners must be disposed exactly once each.
        Assert.Equal(1, donated1.DisposeCount);
        Assert.Equal(1, donated2.DisposeCount);
        // Shells go to the donated-shell freelist (within capacity).
        Assert.Equal(2, pipe._writer.DonatedShellFreelistCount);
    }

    [Fact]
    public async Task Reset_RentedSegmentsInChain_PushedToRentedFreelist()
    {
        var pipe = new Pipely.Pipe(new Pipely.PipeOptions(minimumSegmentSize: 64));
        pipe.Writer.GetMemory(60); pipe.Writer.Advance(60);
        await pipe.Writer.FlushAsync();
        // Reader does NOT drain.

        Assert.NotNull(pipe._writer.ChainHead);
        int freelistBefore = pipe._writer.FreelistCount;

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        // The rented segment in the chain is pushed to the freelist (capacity-permitting).
        Assert.True(pipe._writer.FreelistCount > freelistBefore);
    }

    [Fact]
    public async Task Reset_AllowsFullReuseOfPipeInstance()
    {
        var pipe = new Pipely.Pipe();
        for (int round = 0; round < 3; round++)
        {
            var mem = pipe.Writer.GetMemory(10);
            mem.Span.Fill((byte)round);
            pipe.Writer.Advance(10);
            await pipe.Writer.FlushAsync();

            var r = await pipe.Reader.ReadAsync();
            Assert.Equal(10, r.Buffer.Length);
            Assert.Equal((byte)round, r.Buffer.First.Span[0]);
            pipe.Reader.AdvanceTo(r.Buffer.End);

            pipe.Writer.Complete();
            pipe.Reader.Complete();
            pipe.Reset();
        }
    }

    [Fact]
    public async Task Reset_AfterWriterCompletedWithException_ClearsCompletionException()
    {
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete(new InvalidOperationException("boom"));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipe.Reader.ReadAsync());
        pipe.Reader.Complete();

        pipe.Reset();

        // Post-Reset, no exception in the published writer state.
        Assert.Null(pipe._writer.LastPublishedWriterState.CompletionException);
        Assert.False(pipe._writer.LastPublishedWriterState.IsCompleted);

        // And the pipe is fully usable: the writer state seen by the reader is fresh
        // (no terminal IsCompleted=true sticking around in the triple buffer).
        var mem = pipe.Writer.GetMemory(4); mem.Span.Fill(0x11); pipe.Writer.Advance(4);
        await pipe.Writer.FlushAsync();
        var r = await pipe.Reader.ReadAsync();
        Assert.False(r.IsCompleted);   // Critical: the new reader observes a non-completed state.
        Assert.Equal(4, r.Buffer.Length);
        pipe.Reader.AdvanceTo(r.Buffer.End);
    }

    [Fact]
    public void Reset_OnFreshlyConstructedPipeAfterBothComplete_NoThrow()
    {
        // No GetMemory, no FlushAsync — just complete and Reset.
        var pipe = new Pipely.Pipe();
        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();   // must not throw, must not crash on null ChainHead
    }

    [Fact]
    public void Reset_WithDonatedTail_DisposesAndPoolsTail()
    {
        // The active WritingHead can itself be a donated segment.
        var pipe = new Pipely.Pipe();
        var donated = new TrackingMemoryOwner(40);
        pipe.Writer.Splice(donated);
        // No FlushAsync — the donated segment is the WritingHead.

        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        Assert.Equal(1, donated.DisposeCount);
        Assert.Equal(1, pipe._writer.DonatedShellFreelistCount);
    }

    [Fact]
    public async Task Reset_AfterParkedReadObservedWriterException_ReusableForNewRound()
    {
        // Exercises the live OnCompleted → SetException → Reset → next round path:
        // the reader parks, writer.Complete(ex) signals via SetException, the reader's
        // await re-throws, then we Reset and run a clean round. This is the integration
        // counterpart to the awaiter-level Reset tests.
        var pipe = new Pipely.Pipe();
        var ex = new InvalidOperationException("first round error");

        // Park a read with no data available.
        var readTask = pipe.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        pipe.Writer.Complete(ex);
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await readTask);
        Assert.Same(ex, thrown);

        pipe.Reader.Complete();
        pipe.Reset();

        // Clean round 2: the awaiter has been Reset; new ValueTask tokens are valid.
        var mem = pipe.Writer.GetMemory(8); mem.Span.Fill(0x55); pipe.Writer.Advance(8);
        await pipe.Writer.FlushAsync();
        var r = await pipe.Reader.ReadAsync();
        Assert.False(r.IsCompleted);
        Assert.Equal(8, r.Buffer.Length);
        pipe.Reader.AdvanceTo(r.Buffer.End);
    }

    [Fact]
    public async Task Reset_ReuseAcrossOptionsBoundaries_KeepsOriginalOptions()
    {
        // After Reset, the pipe still uses the options it was constructed with.
        // We can't directly read pipe._options externally without InternalsVisibleTo,
        // but we can verify behavior: the minimumSegmentSize knob is still in effect.
        var pipe = new Pipely.Pipe(new Pipely.PipeOptions(minimumSegmentSize: 64));
        pipe.Writer.GetMemory(1); pipe.Writer.Advance(1);
        await pipe.Writer.FlushAsync();
        var r1 = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(r1.Buffer.End);
        pipe.Writer.Complete();
        pipe.Reader.Complete();
        pipe.Reset();

        // Asking for 1 byte after Reset still rents a >= 64-byte segment.
        var mem = pipe.Writer.GetMemory(1);
        Assert.True(mem.Length >= 64, $"Expected segment of at least 64 bytes; got {mem.Length}.");
    }
}

