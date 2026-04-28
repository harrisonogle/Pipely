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
}
