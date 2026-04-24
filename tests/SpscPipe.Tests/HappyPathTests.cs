using System.Buffers;
using System.IO.Pipelines;
using SpscPipe;

namespace SpscPipe.Tests;

public class HappyPathTests
{
    [Fact]
    public void SingleWriteFlush_AppearsInReader()
    {
        using var pipe = new global::SpscPipe.SpscPipe();
        var writer = pipe.Writer;
        var reader = pipe.Reader;

        // Empty reader — TryRead returns false on a fresh pipe.
        Assert.False(reader.TryRead(out _));

        // Write 4 bytes then flush.
        var mem = writer.GetMemory(4);
        for (var i = 0; i < 4; i++) mem.Span[i] = (byte)(i + 1);
        writer.Advance(4);

        // Fast-path flush (no backpressure, no reader completion).
        var flushTask = writer.FlushAsync();
        Assert.True(flushTask.IsCompletedSuccessfully);
        Assert.False(flushTask.Result.IsCanceled);
        Assert.False(flushTask.Result.IsCompleted);

        // Reader sees the bytes.
        Assert.True(reader.TryRead(out var rr));
        Assert.False(rr.IsCanceled);
        Assert.Equal(4, (int)rr.Buffer.Length);
        var arr = rr.Buffer.ToArray();
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, arr);

        // Advance past the read.
        reader.AdvanceTo(rr.Buffer.End);

        // Nothing left.
        Assert.False(reader.TryRead(out _));
    }

    [Fact]
    public void MultipleFlushes_AppendToChain()
    {
        using var pipe = new global::SpscPipe.SpscPipe(new SpscPipeOptions
        {
            MinimumSegmentSize = 16,
        });
        var writer = pipe.Writer;
        var reader = pipe.Reader;

        for (var round = 0; round < 3; round++)
        {
            var mem = writer.GetMemory(4);
            for (var i = 0; i < 4; i++) mem.Span[i] = (byte)(round * 4 + i + 1);
            writer.Advance(4);
            var t = writer.FlushAsync();
            Assert.True(t.IsCompletedSuccessfully);
        }

        Assert.True(reader.TryRead(out var rr));
        var expected = Enumerable.Range(1, 12).Select(i => (byte)i).ToArray();
        Assert.Equal(expected, rr.Buffer.ToArray());
        reader.AdvanceTo(rr.Buffer.End);
    }

    [Fact]
    public void BufferRotation_CrossesSegmentBoundary()
    {
        // MinimumSegmentSize is small so a larger write triggers rotation.
        using var pipe = new global::SpscPipe.SpscPipe(new SpscPipeOptions
        {
            MinimumSegmentSize = 8,
        });
        var writer = pipe.Writer;
        var reader = pipe.Reader;

        // Fill one buffer.
        var mem = writer.GetMemory(8);
        for (var i = 0; i < 8; i++) mem.Span[i] = (byte)(i + 1);
        writer.Advance(8);

        // Ask for more — forces rotation because remaining = 0.
        var mem2 = writer.GetMemory(8);
        for (var i = 0; i < 8; i++) mem2.Span[i] = (byte)(i + 9);
        writer.Advance(8);

        var t = writer.FlushAsync();
        Assert.True(t.IsCompletedSuccessfully);

        Assert.True(reader.TryRead(out var rr));
        var expected = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        Assert.Equal(expected, rr.Buffer.ToArray());
        reader.AdvanceTo(rr.Buffer.End);
    }

    [Fact]
    public void IncrementalAdvance_RetiresConsumedSegments()
    {
        using var pipe = new global::SpscPipe.SpscPipe(new SpscPipeOptions
        {
            MinimumSegmentSize = 8,
        });
        var writer = pipe.Writer;
        var reader = pipe.Reader;

        // Write three segments' worth.
        for (var round = 0; round < 3; round++)
        {
            var mem = writer.GetMemory(8);
            for (var i = 0; i < 8; i++) mem.Span[i] = (byte)(round * 8 + i + 1);
            writer.Advance(8);
            var t = writer.FlushAsync();
            Assert.True(t.IsCompletedSuccessfully);
        }

        // Consume in chunks of 6 bytes so the advance crosses segment boundaries.
        long consumedTotal = 0;
        for (var i = 0; i < 4; i++)
        {
            Assert.True(reader.TryRead(out var rr));
            var chunk = rr.Buffer.Slice(0, Math.Min(6, rr.Buffer.Length));
            // Sanity check against expected content.
            foreach (var mem in chunk)
            {
                var span = mem.Span;
                for (var j = 0; j < span.Length; j++)
                {
                    Assert.Equal((byte)(consumedTotal + j + 1), span[j]);
                }
                consumedTotal += span.Length;
            }
            reader.AdvanceTo(chunk.End);
            if (consumedTotal >= 24) break;
        }
        Assert.Equal(24, consumedTotal);
    }
}
