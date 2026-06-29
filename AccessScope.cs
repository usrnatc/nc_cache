using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace NcCache;

[StructLayout(LayoutKind.Explicit, Size = 192)]
public class AccessPoint
{
    [FieldOffset(0)]
    public long ReferenceCount;

    [FieldOffset(64)]
    public long LastTimeTouchedUSecs;

    [FieldOffset(128)]
    public long LastUpdateIndexTouched;
}

public readonly struct ExpireParams(long TimeUSecs = 2_000_000, long UpdateIndices = 2)
{
    public readonly long TimeUSecs     = TimeUSecs;
    public readonly long UpdateIndices = UpdateIndices;
}

public static class UpdateTick
{
    private static long __Index;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Index()
    {
        return Volatile.Read(ref __Index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Advance()
    {
        Interlocked.Increment(ref __Index);
    }
}

internal struct Touch
{
    public AccessPoint? Point;
}

public sealed class Access : IDisposable
{
    private const int __InitialCapacity = 16;

    [ThreadStatic] private static Access? __Free;
    private Touch[] __Touches;
    private int __TouchesCount;
    private Access? __NextFree;

    private Access()
    {
        __Touches = new Touch[__InitialCapacity];
        __TouchesCount = 0;
    }

    public static Access Open()
    {
        Access? Acc = __Free;

        if (Acc != null)
        {
            __Free = Acc.__NextFree;
            Acc.__NextFree = null;
            Acc.__TouchesCount = 0;

            return Acc;
        }

        return new Access();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void TouchPoint(AccessPoint Point)
    {
        Interlocked.Increment(ref Point.ReferenceCount);

        long NowUSecs = TimeUtil.NowUSecs();

        if (NowUSecs - Volatile.Read(ref Point.LastTimeTouchedUSecs) > 1_000_000)
            Volatile.Write(ref Point.LastTimeTouchedUSecs, NowUSecs);

        long Tick = UpdateTick.Index();

        if (Tick != Volatile.Read(ref Point.LastUpdateIndexTouched))
            Volatile.Write(ref Point.LastUpdateIndexTouched, Tick);

        if (__TouchesCount >= __Touches.Length)
            Array.Resize(ref __Touches, __Touches.Length * 2);

        __Touches[__TouchesCount++] = new Touch
        {
            Point = Point,
        };
    }

    public void Dispose()
    {
        for (int Index = 0; Index < __TouchesCount; ++Index)
        {
            ref Touch T = ref __Touches[Index];

            if (T.Point != null)
                Interlocked.Decrement(ref T.Point.ReferenceCount);

            T.Point = null;
        }

        __TouchesCount = 0;
        __NextFree = __Free;
        __Free = this;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsExpired(AccessPoint Point, in ExpireParams P)
    {
        long RefCount = Volatile.Read(ref Point.ReferenceCount);
        long LastTimeTouched = Volatile.Read(ref Point.LastTimeTouchedUSecs);
        long LastUpdateTouched = Volatile.Read(ref Point.LastUpdateIndexTouched);

        return (
            RefCount == 0 &&
            LastTimeTouched + P.TimeUSecs <= TimeUtil.NowUSecs() &&
            LastUpdateTouched + P.UpdateIndices <= UpdateTick.Index()
        );
    }
}
