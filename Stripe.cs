using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace NcCache;

[StructLayout(LayoutKind.Explicit, Size = 64)]
public struct Stripe : IDisposable
{
    private const int WRITE_BIT   = 1 << 31;
    private const int READER_MASK = ~WRITE_BIT;

    [FieldOffset(0)]
    internal object __CondVarLock;

    [FieldOffset(8)]
    private int __RWState;

    public Stripe()
    {
        __CondVarLock = new object();
    }

    [UnscopedRef]
    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public ReadScope EnterRead()
    {
        SpinWait Processor = default;

        for (;;)
        {
            int PrevRWStateValue = Volatile.Read(ref __RWState);

            if ((PrevRWStateValue & WRITE_BIT) != 0)
            {
                Processor.SpinOnce();
                continue;
            }

            if (
                Interlocked.CompareExchange(
                    ref __RWState, 
                    PrevRWStateValue + 1, 
                    PrevRWStateValue
                ) == PrevRWStateValue
            ) 
            {
                return new ReadScope(ref __RWState);
            }

            Processor.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    internal void ExitRead()
    {
        Interlocked.Decrement(ref __RWState);
    }

    [UnscopedRef]
    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public WriteScope EnterWrite()
    {
        SpinWait Processor = default;

        for (;;)
        {
            int PrevRWStateValue = Volatile.Read(ref __RWState);

            if ((PrevRWStateValue & WRITE_BIT) != 0)
            {            
                Processor.SpinOnce();
                continue;
            }

            if (
                Interlocked.CompareExchange(
                    ref __RWState, 
                    PrevRWStateValue | WRITE_BIT, 
                    PrevRWStateValue
                ) == PrevRWStateValue
            )
            {
                break;
            }

            Processor.SpinOnce();
        }

        SpinWait DrainProcessor = default;

        while ((Volatile.Read(ref __RWState) & READER_MASK) != 0)
            DrainProcessor.SpinOnce();

        return new WriteScope(ref __RWState);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    internal void ExitWrite()
    {
        Volatile.Write(ref __RWState, 0);
    }

    public void WaitWrite(long EndTimeUSecs)
    {
        lock (__CondVarLock)
        {
            ExitWrite();

            int WaitMSecs = 0;

            if (EndTimeUSecs > TimeUtil.NowUSecs())
                WaitMSecs = (int) Math.Min((EndTimeUSecs - TimeUtil.NowUSecs()) / 1000, int.MaxValue);

            if (WaitMSecs > 0)
                Monitor.Wait(__CondVarLock, WaitMSecs);
        }

        EnterWrite();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public void Broadcast()
    {
        lock (__CondVarLock)
            Monitor.PulseAll(__CondVarLock);
    }

    public void Dispose() { }
}

public ref struct ReadScope
{
    private ref int __RWState;

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    internal ReadScope(ref int RWState)
    {
        __RWState = ref RWState;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        if (!Unsafe.IsNullRef(ref __RWState))
        {
            Interlocked.Decrement(ref __RWState);
            __RWState = ref Unsafe.NullRef<int>();
        }
    }
}

public ref struct WriteScope
{
    private ref int __RWState;

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    internal WriteScope(ref int RWState)
    {
        __RWState = ref RWState;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        if (!Unsafe.IsNullRef(ref __RWState))
        {
            Volatile.Write(ref __RWState, 0);
            __RWState = ref Unsafe.NullRef<int>();
        }
    }
}

public sealed class StripeArray : IDisposable
{
    public readonly Stripe[] Stripes;
    public readonly int      Count;
    private readonly ulong __Mask;

    public StripeArray(int stripeCount)
    {
        Count = stripeCount;

        // NOTE(nathan): assert Count is power of 2
        Debug.Assert(Count != 0 && (Count & (Count - 1)) == 0);

        __Mask = (ulong) (Count - 1);
        Stripes = new Stripe[stripeCount];

        for (int Index = 0; Index < Count; ++Index)
            Stripes[Index] = new Stripe();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public ref Stripe FromSlot(ulong SlotIndex)
    {
        return ref Stripes[SlotIndex & __Mask];
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public int StripeIndex(ulong SlotIndex)
    {
        return (int) (SlotIndex & __Mask);
    }

    public void Dispose()
    {
        for (int Index = 0; Index < Stripes.Length; ++Index)
            Stripes[Index].Dispose();
    }
}

public static class TimeUtil
{
    private static readonly double __USecsPerTick = 1_000_000.0 / Stopwatch.Frequency;

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static long NowUSecs()
    {
        return (long) (Stopwatch.GetTimestamp() * __USecsPerTick);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static long USecsFromSecs(double Secs)
    {
        return (long) (Secs * 1_000_000);
    }
}
