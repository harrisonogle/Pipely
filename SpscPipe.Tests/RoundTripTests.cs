using System.Buffers;
using System.IO.Pipelines;
using System.Text;

namespace SpscPipe.Tests;

// Checkpoint 2: happy-path round-trip tests.
// All tests produce-before-consume in a single thread — awaiter coordination
// arrives in checkpoint 3, so a ReadAsync that finds no data and no completion
// will throw NotImplementedException.
public class RoundTripTests
{
    private static global::SpscPipe.SpscPipe NewPipe(int minSegmentSize = 4096)
    {
        return new global::SpscPipe.SpscPipe(new SpscPipeOptions
        {
            MinimumSegmentSize = minSegmentSize,
            // Thresholds don't matter in checkpoint 2 (no backpressure path).
            PauseWriterThreshold  = 1024 * 1024,
            ResumeWriterThreshold = 512  * 1024,
        });
    }

    private static void Write(PipeWriter writer, ReadOnlySpan<byte> data)
    {
        var mem = writer.GetMemory(data.Length);
        data.CopyTo(mem.Span);
        writer.Advance(data.Length);
    }

    [Fact]
    public async Task SingleWriteFlush_ReadBack_YieldsSameBytes()
    {
        var pipe = NewPipe();
        var payload = Encoding.UTF8.GetBytes("hello");

        Write(pipe.Writer, payload);
        await pipe.Writer.FlushAsync();

        var result = await pipe.Reader.ReadAsync();
        Assert.False(result.IsCanceled);
        Assert.False(result.IsCompleted);
        Assert.Equal(payload, result.Buffer.ToArray());

        pipe.Reader.AdvanceTo(result.Buffer.End);
    }

    [Fact]
    public async Task TwoFlushes_ConcatenatedByReader()
    {
        var pipe = NewPipe();

        Write(pipe.Writer, "foo"u8);
        await pipe.Writer.FlushAsync();

        Write(pipe.Writer, "bar"u8);
        await pipe.Writer.FlushAsync();

        var result = await pipe.Reader.ReadAsync();
        Assert.Equal("foobar"u8.ToArray(), result.Buffer.ToArray());
        pipe.Reader.AdvanceTo(result.Buffer.End);
    }

    [Fact]
    public async Task LargeWrite_RotatesBuffer_ReadBackPreservesBytes()
    {
        // Small MinimumSegmentSize forces buffer rotation inside GetMemory
        // when the payload exceeds remaining capacity.
        var pipe = NewPipe(minSegmentSize: 64);
        var payload = new byte[200];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 256);

        // Write in chunks that each fit, forcing repeated GetMemory/Advance
        // and at least one rotation.
        var writer = pipe.Writer;
        var offset = 0;
        while (offset < payload.Length)
        {
            var chunk = Math.Min(50, payload.Length - offset);
            Write(writer, payload.AsSpan(offset, chunk));
            offset += chunk;
        }
        await writer.FlushAsync();

        var result = await pipe.Reader.ReadAsync();
        Assert.Equal(payload, result.Buffer.ToArray());
        pipe.Reader.AdvanceTo(result.Buffer.End);
    }

    [Fact]
    public async Task PartialAdvance_ReadsRemainderOnNextCall()
    {
        var pipe = NewPipe();

        Write(pipe.Writer, "foobar"u8);
        await pipe.Writer.FlushAsync();

        var firstRead = await pipe.Reader.ReadAsync();
        Assert.Equal(6, firstRead.Buffer.Length);

        // Advance past first 3 bytes only.
        var afterFoo = firstRead.Buffer.GetPosition(3);
        pipe.Reader.AdvanceTo(afterFoo);

        // Write more data and read — the "bar" from the first flush should
        // still be there, followed by the new data.
        Write(pipe.Writer, "baz"u8);
        await pipe.Writer.FlushAsync();

        var secondRead = await pipe.Reader.ReadAsync();
        Assert.Equal("barbaz"u8.ToArray(), secondRead.Buffer.ToArray());
        pipe.Reader.AdvanceTo(secondRead.Buffer.End);
    }

    [Fact]
    public async Task MultipleRotationsWithFlushesInBetween_ReadsFullPayload()
    {
        // Forces a multi-segment published chain: each flush after a
        // rotation splices a chain of length >= 2.
        var pipe = NewPipe(minSegmentSize: 32);

        var payload = new byte[500];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i & 0x7F);

        var writer = pipe.Writer;
        for (var offset = 0; offset < payload.Length; offset += 40)
        {
            var chunk = Math.Min(40, payload.Length - offset);
            Write(writer, payload.AsSpan(offset, chunk));
            if (offset % 80 == 0)
                await writer.FlushAsync();
        }
        await writer.FlushAsync();

        var result = await pipe.Reader.ReadAsync();
        Assert.Equal(payload, result.Buffer.ToArray());
        pipe.Reader.AdvanceTo(result.Buffer.End);
    }

    [Fact]
    public async Task TryRead_WithNoData_ReturnsFalse()
    {
        var pipe = NewPipe();
        var ok = pipe.Reader.TryRead(out var result);
        Assert.False(ok);
        Assert.Equal(default, result.Buffer);
    }

    [Fact]
    public async Task TryRead_AfterFlush_ReturnsDataSynchronously()
    {
        var pipe = NewPipe();
        Write(pipe.Writer, "sync"u8);
        await pipe.Writer.FlushAsync();

        var ok = pipe.Reader.TryRead(out var result);
        Assert.True(ok);
        Assert.Equal("sync"u8.ToArray(), result.Buffer.ToArray());
        pipe.Reader.AdvanceTo(result.Buffer.End);
    }

    [Fact]
    public async Task AdvanceTo_WithoutPriorRead_Throws()
    {
        var pipe = NewPipe();
        Assert.Throws<InvalidOperationException>(
            () => pipe.Reader.AdvanceTo(default));
        await Task.CompletedTask;
    }

    [Fact]
    public async Task ReadWithoutAdvance_SecondReadThrows()
    {
        var pipe = NewPipe();
        Write(pipe.Writer, "x"u8);
        await pipe.Writer.FlushAsync();

        var r1 = await pipe.Reader.ReadAsync();
        Assert.Equal(1, r1.Buffer.Length);

        Assert.Throws<InvalidOperationException>(() => pipe.Reader.TryRead(out _));

        pipe.Reader.AdvanceTo(r1.Buffer.End);
    }

    [Fact]
    public async Task Examined_TracksReaderProgress_AcrossReads()
    {
        // After AdvanceTo(consumed, examined) with examined > consumed,
        // a subsequent read with no new data should NOT report hasNewData.
        var pipe = NewPipe();
        Write(pipe.Writer, "abc"u8);
        await pipe.Writer.FlushAsync();

        var r1 = await pipe.Reader.ReadAsync();
        Assert.Equal(3, r1.Buffer.Length);

        // Consumed = start (nothing), examined = end (everything).
        pipe.Reader.AdvanceTo(r1.Buffer.Start, r1.Buffer.End);

        // No new data arrived; TryRead must report false.
        Assert.False(pipe.Reader.TryRead(out _));

        // Now write more; TryRead returns the full unread buffer.
        Write(pipe.Writer, "def"u8);
        await pipe.Writer.FlushAsync();

        Assert.True(pipe.Reader.TryRead(out var r2));
        Assert.Equal("abcdef"u8.ToArray(), r2.Buffer.ToArray());
        pipe.Reader.AdvanceTo(r2.Buffer.End);
    }
}
