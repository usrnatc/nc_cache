using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NcCache;

[StructLayout(LayoutKind.Sequential)]
[method: MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
public readonly struct Hash128(ulong Lo, ulong Hi) : IEquatable<Hash128>
{
    public readonly ulong Lo = Lo;
    public readonly ulong Hi = Hi;

    public bool IsZero
    {
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        get => Lo == 0 && Hi == 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public bool Equals(Hash128 Other)
    {
        return Lo == Other.Lo && Hi == Other.Hi;
    }

    public override bool Equals(object? Obj)
    {
        return Obj is Hash128 Other && Equals(Other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Lo, Hi);
    }

    public override string ToString()
    {
        return $"{Hi:x16}{Lo:x16}";
    }

    public static bool operator ==(Hash128 ValueA, Hash128 ValueB)
    {
        return ValueA.Lo == ValueB.Lo && ValueA.Hi == ValueB.Hi;
    }

    public static bool operator !=(Hash128 ValueA, Hash128 ValueB)
    {
        return !(ValueA == ValueB);
    }
    
    public static readonly Hash128 Zero = default;
}