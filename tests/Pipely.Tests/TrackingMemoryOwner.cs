// tests/Pipely.Tests/TrackingMemoryOwner.cs
using System.Buffers;

namespace Pipely.Tests;

internal sealed class TrackingMemoryOwner : IMemoryOwner<byte>
{
    private readonly byte[] _bytes;
    private bool _disposed;

    public int DisposeCount;

    public TrackingMemoryOwner(int size) { _bytes = new byte[size]; }
    public TrackingMemoryOwner(byte[] bytes) { _bytes = bytes; }

    public Memory<byte> Memory =>
        _disposed
            ? throw new ObjectDisposedException(nameof(TrackingMemoryOwner))
            : _bytes;

    public void Dispose()
    {
        DisposeCount++;
        _disposed = true;
    }

    // Unchecked accessor for tests that need to inspect bytes after Dispose.
    public byte[] UnderlyingBytes => _bytes;
}
