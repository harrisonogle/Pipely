using System.Buffers;
using System.IO.Pipelines;

namespace SpscPipe.Tests;

public class LifecycleTests
{
    [Fact]
    public async Task WriterComplete_FlushesPendingBytes_ReaderSeesAll()
    {
        var pipe = new global::SpscPipe.SpscPipe();
        try
        {
            var mem = pipe.Writer.GetMemory(5);
            for (var i = 0; i < 5; i++) mem.Span[i] = (byte)(i + 10);
            pipe.Writer.Advance(5);

            // Complete without an explicit FlushAsync — §6.6 step 1 should
            // flush the pending bytes.
            pipe.Writer.Complete();

            var rr = await pipe.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));
            Assert.True(rr.IsCompleted);
            Assert.Equal(5, (int)rr.Buffer.Length);
            Assert.Equal(new byte[] { 10, 11, 12, 13, 14 }, BuffersExt.ToArray(rr.Buffer));
            pipe.Reader.AdvanceTo(rr.Buffer.End);
        }
        finally { pipe.Dispose(); }
    }

    [Fact]
    public async Task WriterComplete_WithException_PropagatesOnDrainedReadAsync()
    {
        var pipe = new global::SpscPipe.SpscPipe();
        try
        {
            var mem = pipe.Writer.GetMemory(2);
            mem.Span[0] = 1; mem.Span[1] = 2;
            pipe.Writer.Advance(2);
            pipe.Writer.Complete(new InvalidOperationException("writer failed"));

            // §10.4: ReadAsync returning buffer+IsCompleted=true does NOT throw.
            var rr = await pipe.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));
            Assert.True(rr.IsCompleted);
            Assert.Equal(2, (int)rr.Buffer.Length);
            pipe.Reader.AdvanceTo(rr.Buffer.End);

            // Next ReadAsync drained and writer-completed-with-ex: throws.
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await pipe.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2)));
        }
        finally { pipe.Dispose(); }
    }

    [Fact]
    public async Task ReaderComplete_BypassesHysteresis_WakesParkedWriter()
    {
        var pipe = new global::SpscPipe.SpscPipe(new SpscPipeOptions
        {
            MinimumSegmentSize = 8,
            PauseWriterThreshold = 8,
            ResumeWriterThreshold = 4,
        });
        try
        {
            // Fill to exceed Pause.
            var mem = pipe.Writer.GetMemory(16);
            for (var i = 0; i < 16; i++) mem.Span[i] = (byte)(i + 1);
            pipe.Writer.Advance(16);
            var flushTask = pipe.Writer.FlushAsync().AsTask();
            Assert.False(flushTask.IsCompleted);  // parked

            // Reader Complete — bypasses hysteresis per §8.5.
            pipe.Reader.Complete();

            var result = await flushTask.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.True(result.IsCompleted);
            Assert.False(result.IsCanceled);
        }
        finally { pipe.Dispose(); }
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        var pipe = new global::SpscPipe.SpscPipe();
        pipe.Dispose();
        pipe.Dispose();  // no throw
    }

    [Fact]
    public void Dispose_AfterWrite_ReleasesHolder()
    {
        var pipe = new global::SpscPipe.SpscPipe();
        var mem = pipe.Writer.GetMemory(1);
        mem.Span[0] = 0xFF;
        pipe.Writer.Advance(1);
        pipe.Writer.FlushAsync().AsTask().GetAwaiter().GetResult();
        pipe.Dispose();
        // Smoke test — no throw, no assertion failure.
    }
}
