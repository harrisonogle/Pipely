namespace SpscPipelines;

internal struct WriterState
{
    public BufferSegment? HeadSegment;
    public BufferSegment? TailSegment;
    public int            TailWritten;
    public long           TotalWritten;
    public bool           IsCompleted;
    public Exception?     CompletionException;
}
