using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace NcCache;

public sealed class LaneGroup : IDisposable
{
    [ThreadStatic] private static int __Index;
    [ThreadStatic] private static int __Count = 1;
    [ThreadStatic] private static LaneGroup? __Current;
    private readonly int __LaneCount;
    private readonly Barrier __Barrier;
    private long __Broadcast64;
    private object? __BroadcastObj;
    private long __Accumulator;

    public static int LaneIndex()
    {
        return __Index;
    }

    public static int LaneCount()
    {
        return __Count;
    }

    public static LaneGroup? Current()
    {
        return __Current;
    }

    public int Count()
    {
        return __LaneCount;
    }

    public LaneGroup(int Count)
    {
        __LaneCount = Math.Max(1, Count);
        __Barrier = new Barrier(__LaneCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public void SetThreadContext(int Index)
    {
        __Index = Index;
        __Count = __LaneCount;
        __Current = this;
    }

    public static LaneScopeState PushSingleLane()
    {
        var State = new LaneScopeState(__Index, __Count, __Current);

        __Index = 0;
        __Count = 1;
        __Current = null;

        return State;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static void PopLane(in LaneScopeState State)
    {
        __Index = State.PrevIndex;
        __Count = State.PrevCount;
        __Current = State.PrevGroup;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public void Sync()
    {
        __Barrier.SignalAndWait();
    }

    public long SyncLong(long Value, int SourceLane)
    {
        if (__Index == SourceLane)
            Volatile.Write(ref __Broadcast64, Value);

        __Barrier.SignalAndWait();

        long Result = Volatile.Read(ref __Broadcast64);

        __Barrier.SignalAndWait();

        return Result;
    }

    public T? SyncObj<T>(T? Value, int SourceLane) where T : class
    {
        if (__Index == SourceLane)
            Volatile.Write(ref __BroadcastObj, Value);

        __Barrier.SignalAndWait();

        T? Result = (T?) Volatile.Read(ref __BroadcastObj);

        __Barrier.SignalAndWait();

        return Result;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public void ResetAccumulator()
    {
        Volatile.Write(ref __Accumulator, 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public void AccumulateAdd(long Value)
    {
        Interlocked.Add(ref __Accumulator, Value);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public long SyncAndReadAccumulator()
    {
        __Barrier.SignalAndWait();

        return Volatile.Read(ref __Accumulator);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public static (int Min, int Max) LaneRange(long TotalCount)
    {
        return ComputeRange(__Index, __Count, TotalCount);
    }

    public static (int Min, int Max) ComputeRange(
        int LaneIndex,
        int LaneCount,
        long TotalCount
    )
    {
        long PerLane = TotalCount / LaneCount;
        long Remaining = TotalCount - PerLane * LaneCount;
        long RemainingClamped = Math.Min(LaneIndex, Remaining);
        long BaseIndex = LaneIndex * PerLane + RemainingClamped;
        long BaseClamped = Math.Min(BaseIndex, TotalCount);
        long EndIndex = BaseClamped + PerLane + (LaneIndex < Remaining ? 1 : 0);
        long EndClamped = Math.Min(EndIndex, TotalCount);

        return ((int) BaseClamped, (int) EndClamped);
    }

    public void Dispose()
    {
        __Barrier.Dispose();
    }
}

[method: MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
public readonly struct LaneScopeState(int Index, int Count, LaneGroup? Group)
{
    public readonly int PrevIndex = Index;
    public readonly int PrevCount = Count;
    public readonly LaneGroup? PrevGroup = Group;
}