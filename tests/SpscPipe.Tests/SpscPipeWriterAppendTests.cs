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
        int  end1Before     = donated1.End;
        long ri1Before      = donated1.RunningIndex;

        pipe.Writer.Append(owner2);
        var donated2 = pipe._writingHead!;

        // donated1's End/Memory unchanged (idempotent Freeze write).
        Assert.Equal(end1Before, donated1.End);
        Assert.Equal(ri1Before, donated1.RunningIndex);
        Assert.Same(donated2, donated1.Next);
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
}
