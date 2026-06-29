using System;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NcCache;

public static class Hashing
{
    private const long __Seed64 = unchecked((long) 0x517CC1B727220A95UL);

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static ulong Hash64(ReadOnlySpan<byte> Data)
    {
        return Hash64(__Seed64, Data);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static ulong Hash64(long Seed, ReadOnlySpan<byte> Data)
    {
        return XxHash3.HashToUInt64(Data, Seed);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static Hash128 Hash128(ReadOnlySpan<byte> Data)
    {
        return Hash128(__Seed64, Data);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static Hash128 Hash128(long Seed, ReadOnlySpan<byte> Data)
    {
        Span<byte> HashBytes = stackalloc byte[16];

        XxHash128.Hash(Data, HashBytes, Seed);

        ulong Lo = MemoryMarshal.Read<ulong>(HashBytes[.. 8]);
        ulong Hi = MemoryMarshal.Read<ulong>(HashBytes[8 ..]);

        return new Hash128(Lo, Hi);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static ulong HashStruct<T>(in T Value) where T : unmanaged
    {
        ReadOnlySpan<byte> Data = MemoryMarshal.AsBytes(
            MemoryMarshal.CreateReadOnlySpan(in Value, 1)
        );

        return Hash64(Data);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static ulong HashString(string String)
    {
        return Hash64(MemoryMarshal.AsBytes(String.AsSpan()));
    }
}