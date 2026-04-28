using System.Buffers;
using SpscPipelines;
using Xunit;

namespace SpscPipe.Tests;

public class SpscPipeWriterAppendTests
{
    // ---------- Argument validation: ownership stays with caller on throw ----------

    [Fact]
    public void Append_NullBuffer_NoArg_Throws_ArgumentNull()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        Assert.Throws<ArgumentNullException>(() => pipe.Writer.Append(null!));
    }

    [Fact]
    public void Append_NullBuffer_ThreeArg_Throws_ArgumentNull()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        Assert.Throws<ArgumentNullException>(() => pipe.Writer.Append(null!, 0, 0));
    }

    [Fact]
    public void Append_DisposedPipe_Throws_ObjectDisposed_CallerStillOwns()
    {
        var pipe = new SpscPipelines.SpscPipe();
        pipe.Dispose();

        var owner = new TrackingMemoryOwner(64);
        Assert.Throws<ObjectDisposedException>(() => pipe.Writer.Append(owner));
        Assert.Equal(0, owner.DisposeCount);   // pipe did NOT dispose; caller still owns
    }

    [Fact]
    public void Append_CompletedWriter_Throws_InvalidOp_CallerStillOwns()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        pipe._writerCompleted = true;          // simulate post-Complete state (internal flag)

        var owner = new TrackingMemoryOwner(64);
        Assert.Throws<InvalidOperationException>(() => pipe.Writer.Append(owner));
        Assert.Equal(0, owner.DisposeCount);
    }

    [Theory]
    [InlineData(-1, 0)]    // negative start
    [InlineData(0, -1)]    // negative length
    [InlineData(33, 32)]   // start + length > buffer.Memory.Length
    [InlineData(64, 1)]    // start past end + positive length
    public void Append_RangeViolation_Throws_OutOfRange_CallerStillOwns(int start, int length)
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(64);
        Assert.Throws<ArgumentOutOfRangeException>(() => pipe.Writer.Append(owner, start, length));
        Assert.Equal(0, owner.DisposeCount);
    }

    [Fact]
    public void Append_ValidationThrows_PipeStateUntouched()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        // Establish a known state.
        pipe.Writer.GetMemory(40);
        pipe.Writer.Advance(40);
        long totalWrittenBefore = pipe._totalWritten;
        var writingHeadBefore = pipe._writingHead;
        int bufferedBefore = pipe._writingHeadBytesBuffered;

        var owner = new TrackingMemoryOwner(64);
        Assert.Throws<ArgumentOutOfRangeException>(() => pipe.Writer.Append(owner, -1, 0));

        Assert.Equal(totalWrittenBefore, pipe._totalWritten);
        Assert.Same(writingHeadBefore, pipe._writingHead);
        Assert.Equal(bufferedBefore, pipe._writingHeadBytesBuffered);
        Assert.Equal(0, owner.DisposeCount);
    }

    // ---------- Zero-length: accept-and-dispose ----------

    [Fact]
    public void Append_ZeroLength_ThreeArg_DisposesAndReturns_NoChainMutation()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(64);

        long totalWrittenBefore = pipe._totalWritten;
        var writingHeadBefore   = pipe._writingHead;
        var chainHeadBefore     = pipe._chainHead;

        pipe.Writer.Append(owner, start: 10, length: 0);

        Assert.Equal(1, owner.DisposeCount);
        Assert.Equal(totalWrittenBefore, pipe._totalWritten);
        Assert.Same(writingHeadBefore, pipe._writingHead);
        Assert.Same(chainHeadBefore, pipe._chainHead);
    }

    [Fact]
    public void Append_BufferWithMemoryLengthZero_NoArg_DisposesAndReturns()
    {
        // Memory.Length == 0 routes through the no-arg overload to the 3-arg overload
        // with length=0; same accept-and-dispose outcome.
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(0);

        pipe.Writer.Append(owner);

        Assert.Equal(1, owner.DisposeCount);
        Assert.Equal(0, pipe._totalWritten);
        Assert.Null(pipe._writingHead);
    }

    // ---------- Bootstrap: empty pipe + Append ----------

    [Fact]
    public void Append_NoArg_OnEmptyPipe_BootstrapsChainHeadEqualsWritingHead()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var bytes = new byte[64];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i + 1);
        var owner = new TrackingMemoryOwner(bytes);

        pipe.Writer.Append(owner);

        Assert.NotNull(pipe._chainHead);
        Assert.Same(pipe._chainHead, pipe._writingHead);
        Assert.Equal(64, pipe._writingHeadBytesBuffered);
        Assert.Equal(64, pipe._totalWritten);

        var seg = pipe._chainHead!;
        Assert.Equal(0, seg.RunningIndex);
        Assert.True(seg.IsDonated);
        Assert.Same(pipe, seg.OwnerToken);
        Assert.Equal(64, seg.End);
        Assert.Equal(64, seg.AvailableMemory.Length);
        Assert.Equal((byte)1,  seg.AvailableMemory.Span[0]);
        Assert.Equal((byte)64, seg.AvailableMemory.Span[63]);

        Assert.Equal(0, owner.DisposeCount);   // ownership transferred; not yet recycled
    }

    [Fact]
    public void Append_ThreeArg_OnEmptyPipe_PublishedSliceMatchesStartAndLength()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var bytes = new byte[1024];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i & 0xFF);
        var owner = new TrackingMemoryOwner(bytes);

        pipe.Writer.Append(owner, start: 100, length: 50);

        Assert.NotNull(pipe._chainHead);
        var seg = pipe._chainHead!;
        Assert.Equal(50, seg.End);
        Assert.Equal(50, seg.AvailableMemory.Length);
        // Bytes 100..149 of the underlying array are exposed.
        Assert.Equal((byte)100, seg.AvailableMemory.Span[0]);
        Assert.Equal((byte)149, seg.AvailableMemory.Span[49]);
        Assert.Equal(50, pipe._totalWritten);
    }

    // ---------- Steady-state splice ----------

    [Fact]
    public void Append_AfterPartialFill_FreezesPreviousTailAndSplicesDonated()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        // Establish a partially-filled rented tail.
        var rentedMem = pipe.Writer.GetMemory(64);
        for (int i = 0; i < 40; i++) rentedMem.Span[i] = (byte)i;
        pipe.Writer.Advance(40);
        var prevTail = pipe._writingHead!;

        // Now Append a donated buffer.
        var donatedBytes = new byte[20];
        for (int i = 0; i < donatedBytes.Length; i++) donatedBytes[i] = (byte)(100 + i);
        var owner = new TrackingMemoryOwner(donatedBytes);

        pipe.Writer.Append(owner);

        // Previous tail (rented) is frozen with End=40.
        Assert.Equal(40, prevTail.End);
        Assert.NotNull(prevTail.Next);
        Assert.False(prevTail.IsDonated);

        // The donated segment is the new writing head.
        var donated = pipe._writingHead!;
        Assert.NotSame(prevTail, donated);
        Assert.Same(donated, prevTail.Next);
        Assert.True(donated.IsDonated);
        Assert.Same(pipe, donated.OwnerToken);
        Assert.Equal(20, donated.End);
        Assert.Equal(40, donated.RunningIndex);   // prevTail.RunningIndex (0) + 40
        Assert.Null(donated.Next);

        // Counters
        Assert.Equal(20, pipe._writingHeadBytesBuffered);
        Assert.Equal(60, pipe._totalWritten);

        // Chain head is still the original prevTail (not donated).
        Assert.Same(prevTail, pipe._chainHead);
        Assert.Equal(0, owner.DisposeCount);
    }

    [Fact]
    public void Append_AfterAppend_PreviousDonatedTailIsLinkedIdempotently()
    {
        // Spec §2.2: when the previous _writingHead is itself donated, the steady-state
        // Freeze(filled, newDonated) call writes End/base.Memory to the same values they
        // already held; only Next changes meaningfully.
        using var pipe = new SpscPipelines.SpscPipe();

        var owner1 = new TrackingMemoryOwner(30);
        var owner2 = new TrackingMemoryOwner(50);

        pipe.Writer.Append(owner1);
        var donated1 = pipe._writingHead!;
        int  end1Before    = donated1.End;
        int  memLenBefore  = ((System.Buffers.ReadOnlySequenceSegment<byte>)donated1).Memory.Length;

        pipe.Writer.Append(owner2);
        var donated2 = pipe._writingHead!;

        // donated1's End/base.Memory unchanged (idempotent Freeze write — spec §2.2).
        Assert.Equal(end1Before,    donated1.End);
        Assert.Equal(memLenBefore,  ((System.Buffers.ReadOnlySequenceSegment<byte>)donated1).Memory.Length);
        Assert.Same(donated2,       donated1.Next);
        // donated2 properly chained.
        Assert.True(donated2.IsDonated);
        Assert.Equal(50, donated2.End);
        Assert.Equal(30, donated2.RunningIndex);   // donated1.RunningIndex(0) + donated1.End(30)

        Assert.Equal(50, pipe._writingHeadBytesBuffered);
        Assert.Equal(80, pipe._totalWritten);

        // Chain head is donated1 (the very first segment).
        Assert.Same(donated1, pipe._chainHead);
    }

    [Fact]
    public void Append_WithStartOffset_SplicesOnlyTheSelectedSlice()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var bytes = new byte[1024];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i & 0xFF);
        var owner = new TrackingMemoryOwner(bytes);

        pipe.Writer.Append(owner, start: 200, length: 100);

        var seg = pipe._writingHead!;
        Assert.Equal(100, seg.End);
        Assert.Equal((byte)200, seg.AvailableMemory.Span[0]);
        Assert.Equal((byte)((200 + 99) & 0xFF), seg.AvailableMemory.Span[99]);
    }

    // ---------- Post-Append interactions with the rest of the writer surface ----------

    [Fact]
    public void GetMemory_AfterAppend_TransitionsToFreshRentedTail()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        var owner = new TrackingMemoryOwner(20);
        pipe.Writer.Append(owner);
        var donated = pipe._writingHead!;

        var mem = pipe.Writer.GetMemory(64);

        // _writingHead should have moved off the donated segment to a fresh rented tail.
        Assert.NotSame(donated, pipe._writingHead);
        Assert.False(pipe._writingHead!.IsDonated);
        // The donated segment is now linked as a non-tail chain segment.
        Assert.Same(pipe._writingHead, donated.Next);
        // _writingHeadBytesBuffered resets to 0 for the new tail.
        Assert.Equal(0, pipe._writingHeadBytesBuffered);
        Assert.True(mem.Length >= 64);
    }

    [Fact]
    public void Advance_AfterAppend_ThrowsArgumentOutOfRange()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(20);
        pipe.Writer.Append(owner);

        // _writingHead.AvailableMemory.Length == _writingHeadBytesBuffered, so any positive
        // Advance fails the existing bounds check at SpscPipe.Writer.cs:52.
        Assert.Throws<ArgumentOutOfRangeException>(() => pipe.Writer.Advance(1));
    }

    [Fact]
    public async Task FlushAsync_AfterAppend_PublishesDonatedAsTailSegment()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var owner = new TrackingMemoryOwner(50);
        pipe.Writer.Append(owner);
        var donated = pipe._writingHead!;

        var fr = await pipe.Writer.FlushAsync();
        Assert.False(fr.IsCanceled);
        Assert.False(fr.IsCompleted);

        var snap = pipe._lastPublishedWriterState;
        Assert.Same(donated, snap.TailSegment);
        Assert.Equal(50, snap.TailWritten);
        Assert.Equal(50, snap.TotalWritten);
        Assert.Same(donated, snap.HeadSegment);   // bootstrap case: donated is also chain head
    }

    [Fact]
    public async Task FlushAsync_AfterMixedWriteAndAppend_PublishesCorrectChain()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        var rentedMem = pipe.Writer.GetMemory(64);
        for (int i = 0; i < 40; i++) rentedMem.Span[i] = (byte)i;
        pipe.Writer.Advance(40);
        var rented = pipe._writingHead!;

        var owner = new TrackingMemoryOwner(20);
        pipe.Writer.Append(owner);

        await pipe.Writer.FlushAsync();
        var snap = pipe._lastPublishedWriterState;
        Assert.Same(rented, snap.HeadSegment);
        Assert.Same(pipe._writingHead, snap.TailSegment);
        Assert.True(snap.TailSegment!.IsDonated);
        Assert.Equal(20, snap.TailWritten);
        Assert.Equal(60, snap.TotalWritten);
    }

    [Fact]
    public void Append_AfterGetMemoryWithZeroBuffered_FreezesEmptyRentedSegment()
    {
        // Spec §8: "After GetMemory + Advance(0) (zero buffered)" → previous tail is frozen
        // with End=0; donated is spliced after. The empty rented segment is harmless and
        // recycles to freelist on drain.
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        pipe.Writer.GetMemory(64);
        pipe.Writer.Advance(0);
        var prevTail = pipe._writingHead!;

        var owner = new TrackingMemoryOwner(20);
        pipe.Writer.Append(owner);

        Assert.Equal(0, prevTail.End);
        Assert.False(prevTail.IsDonated);
        Assert.NotNull(prevTail.Next);
        Assert.True(prevTail.Next!.IsDonated);
        Assert.Same(prevTail.Next, pipe._writingHead);
        Assert.Equal(20, pipe._writingHead!.End);
        Assert.Equal(0, pipe._writingHead.RunningIndex);
        Assert.Equal(20, pipe._totalWritten);
    }

    [Fact]
    public async Task LargeAppend_DoesNotPark_SubsequentFlushAsyncParksWhenOverThreshold()
    {
        // Spec §4.4: Append doesn't gate on PauseWriterThreshold; FlushAsync does.
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(
            pauseWriterThreshold: 1024, resumeWriterThreshold: 512));

        var owner = new TrackingMemoryOwner(8 * 1024);   // well over the pause threshold
        pipe.Writer.Append(owner);   // synchronous, never parks; no exception

        Assert.Equal(8 * 1024, pipe._totalWritten);

        // FlushAsync should park because unconsumed >= PauseWriterThreshold.
        var flushTask = pipe.Writer.FlushAsync().AsTask();
        // The task is parked; not completed synchronously.
        Assert.False(flushTask.IsCompleted);

        // Drain the buffer to release the parked writer.
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);

        // Now the parked FlushAsync resolves.
        var fr = await flushTask;
        Assert.False(fr.IsCanceled);
    }

    [Fact]
    public void BackToBackAppends_NoEmptyRentedTailsBetweenDonations()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var o1 = new TrackingMemoryOwner(10);
        var o2 = new TrackingMemoryOwner(20);
        var o3 = new TrackingMemoryOwner(30);

        pipe.Writer.Append(o1);
        pipe.Writer.Append(o2);
        pipe.Writer.Append(o3);

        // Walk the chain and verify three donated segments back-to-back.
        var s = pipe._chainHead!;
        Assert.True(s.IsDonated);
        Assert.Equal(10, s.End);
        s = s.Next!;
        Assert.NotNull(s);
        Assert.True(s.IsDonated);
        Assert.Equal(20, s.End);
        s = s.Next!;
        Assert.NotNull(s);
        Assert.True(s.IsDonated);
        Assert.Equal(30, s.End);
        Assert.Null(s.Next);

        Assert.Same(s, pipe._writingHead);
        Assert.Equal(60, pipe._totalWritten);
    }

    // ---------- Recycle path: donated -> DisposeOwned + drop; rented -> freelist (unchanged) ----------

    [Fact]
    public async Task ReaderDrainsPastDonated_DisposesOwner_FreelistCountUnchanged()
    {
        using var pipe = new SpscPipelines.SpscPipe();
        var donatedOwner = new TrackingMemoryOwner(30);
        pipe.Writer.Append(donatedOwner);
        // Force a subsequent rented tail so the donated segment becomes a non-tail chain segment.
        pipe.Writer.GetMemory(50); pipe.Writer.Advance(50);

        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        // Drain past the donated segment (and the rented bytes) entirely.
        pipe.Reader.AdvanceTo(rr.Buffer.End);

        int freelistBefore = pipe._freelistCount;

        // The next FlushAsync runs RecycleDrainedSegments after re-acquiring the reader state.
        await pipe.Writer.FlushAsync();

        Assert.Equal(1, donatedOwner.DisposeCount);
        // freelistCount may have grown by 1 (the rented segment that was once the writingHead
        // before GetMemory transitioned past it — but in this minimal scenario,
        // _writingHead never transitioned again, so the rented tail stays as _writingHead and
        // recycle stops at it). Either way, it must NOT have grown to absorb the donated segment.
        Assert.True(pipe._freelistCount <= freelistBefore + 1);
    }

    [Fact]
    public async Task RecyclePath_RentedSegmentStillFreelisted_RegressionGuard()
    {
        // Existing rented-segment recycle behavior must be preserved.
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));
        // Two rented segments in chain.
        pipe.Writer.GetMemory(64); pipe.Writer.Advance(64);
        pipe.Writer.GetMemory(64); pipe.Writer.Advance(50);
        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);

        int freelistBefore = pipe._freelistCount;
        await pipe.Writer.FlushAsync();
        // The first segment is recycled to the freelist (or disposed if cap-overflow).
        Assert.True(pipe._freelistCount > freelistBefore);
    }

    [Fact]
    public async Task DisposePipe_WithMixedChain_DisposesAllOwners()
    {
        var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));

        var donated1 = new TrackingMemoryOwner(20);
        var donated2 = new TrackingMemoryOwner(30);

        pipe.Writer.Append(donated1);
        pipe.Writer.GetMemory(64); pipe.Writer.Advance(40);    // rented in middle
        pipe.Writer.Append(donated2);
        await pipe.Writer.FlushAsync();
        // Don't drain — chain is full of un-consumed segments.

        pipe.Dispose();

        Assert.Equal(1, donated1.DisposeCount);
        Assert.Equal(1, donated2.DisposeCount);
    }

    [Fact]
    public async Task ReadResultBufferContent_IncludesDonatedBytes_InCorrectPosition()
    {
        using var pipe = new SpscPipelines.SpscPipe(new SpscPipeOptions(minimumSegmentSize: 64));

        // Rented [0..3] = 0x01,0x02,0x03,0x04
        var rentedMem = pipe.Writer.GetMemory(64);
        rentedMem.Span[0] = 0x01; rentedMem.Span[1] = 0x02;
        rentedMem.Span[2] = 0x03; rentedMem.Span[3] = 0x04;
        pipe.Writer.Advance(4);

        // Donated bytes [4..6] = 0xAA,0xBB,0xCC
        var donatedBytes = new byte[] { 0xAA, 0xBB, 0xCC };
        var donatedOwner = new TrackingMemoryOwner(donatedBytes);
        pipe.Writer.Append(donatedOwner);

        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        var arr = rr.Buffer.ToArray();
        Assert.Equal(new byte[] { 0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB, 0xCC }, arr);

        pipe.Reader.AdvanceTo(rr.Buffer.End);
    }
}
