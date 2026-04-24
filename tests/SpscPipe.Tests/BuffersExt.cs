using System.Buffers;

namespace SpscPipe.Tests;

internal static class BuffersExt
{
    internal static byte[] ToArray(ReadOnlySequence<byte> seq)
    {
        var arr = new byte[(int)seq.Length];
        seq.CopyTo(arr);
        return arr;
    }
}
