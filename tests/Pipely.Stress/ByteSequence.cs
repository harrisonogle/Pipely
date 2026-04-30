namespace PipelyStress;

internal static class ByteSequence
{
    // Deterministic, O(1)-per-byte, allocation-free. Producer and consumer compute the
    // same expected byte from the absolute offset alone — no shared RNG state needed.
    // Uses Knuth's multiplicative hash; quality is sufficient for byte-integrity checks.
    public static byte ByteAt(long offset)
    {
        ulong x = unchecked((ulong)offset);
        x = unchecked(x * 2654435761UL);
        x ^= x >> 16;
        return (byte)x;
    }
}
