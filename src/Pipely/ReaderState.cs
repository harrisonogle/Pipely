namespace Pipely;

internal struct ReaderState
{
    public BufferSegment? HeadSegment;
    public long           TotalConsumed;
    public long           TotalExamined;
    public bool           IsCompleted;
    public Exception?     CompletionException;
}
