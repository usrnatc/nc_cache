using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace NcCache;

public readonly struct CacheKeyData : IEquatable<CacheKeyData>
{
    public readonly string Path;
    public readonly long   Offset;
    public readonly long   Length;
    public readonly ulong  Hash;
    public readonly ulong  PathHash;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CacheKeyData(string Path, long Offset, long Length, ulong PathHash)
    {
        this.Path     = Path;
        this.Offset   = Offset;
        this.Length   = Length;
        this.PathHash = PathHash;

        Span<byte> Buf = stackalloc byte[24];

        BinaryPrimitives.WriteUInt64LittleEndian(Buf, PathHash);
        BinaryPrimitives.WriteInt64LittleEndian(Buf[8..], Offset);
        BinaryPrimitives.WriteInt64LittleEndian(Buf[16..], Length);
        this.Hash = Hashing.Hash64(Buf);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(CacheKeyData Other)
    {
        return (
            Hash == Other.Hash &&
            Path == Other.Path &&
            Offset == Other.Offset &&
            Length == Other.Length
        );
    }

    public override bool Equals(object? Obj) => Obj is CacheKeyData O && Equals(O);
    public override int GetHashCode() => HashCode.Combine(Hash);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator==(CacheKeyData ValueA, CacheKeyData ValueB) => ValueA.Equals(ValueB);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator!=(CacheKeyData ValueA, CacheKeyData ValueB) => !ValueA.Equals(ValueB);
}

public readonly struct CacheResult<T>(
    T Value, 
    bool IsFound, 
    bool IsStale
) where T : struct
{
    public readonly T Value = Value;
    public readonly bool IsFound = IsFound;
    public readonly bool IsStale = IsStale;

    public static CacheResult<T> Miss()
    {
        return new(default, false, true);
    }
}

public delegate T ObjectCreateFuncType<T>(
    CacheKeyData Key,
    CancelToken Cancel,
    out bool Retry,
    ref ulong Generation
) where T : struct;

public delegate void ObjectDestroyFuncType<T>(T Obj) where T : struct;

[Flags]
public enum CacheFlags : uint
{
    None         = 0,
    WaitForFresh = 1 << 0,
    HighPriority = 1 << 1,
    Wide         = 1 << 2
}

public struct CacheLookupParams
{
    public ulong Generation;
    public long EvictionThresholdUSecs;
    public CacheFlags Flags;

    public static CacheLookupParams Default()
    {
        return new()
        {
            Generation = 0,
            EvictionThresholdUSecs = 2_000_000,
            Flags = CacheFlags.None
        };
    }
}

internal sealed class OCNode<T> where T : struct
{
    public OCNode<T>? Next;
    public OCNode<T>? Prev;
    public CacheKeyData Key;
    public long LastRequestedGeneration;
    public long LastCompletedGeneration;
    public T Value;
    public AccessPoint Point = new();
    public long WorkingCount;
    public long CompletionCount;
    public long EvictionThesholdUSecs;
    public CancelToken Cancel = new();

    public void Reset()
    {
        Next = null;
        Prev = null;
        Key = default;
        LastRequestedGeneration = 0;
        LastCompletedGeneration = 0;
        Value = default;
        Point = new AccessPoint();
        WorkingCount = 0;
        CompletionCount = 0;
        EvictionThesholdUSecs = 0;
        Cancel = new CancelToken();
    }
}

internal sealed class OCSlot<T> where T : struct
{
    public OCNode<T>? Head;
    public OCNode<T>? Tail;
    public OCNode<T>? Free;
}

internal struct OCRequest<T> where T : struct
{
    public CacheKeyData Key;
    public ulong Generation;
    public OCNode<T> TargetNode;
    public ObjectCreateFuncType<T> CreateFunc;
    public bool IsWide;
}

public interface IObjectCache
{
    void Evict((int Min, int Max) SlotRange);
    void CancelExpired();
    void ProcessRequests(LaneGroup? Group);

    int SlotCount { get; }
}

public sealed class ObjectCache<T> : IObjectCache, IDisposable where T : struct
{
    private readonly ObjectCreateFuncType<T> __CreateFunc;
    private readonly ObjectDestroyFuncType<T>? __DestroyFunc;
    private readonly int __SlotsCount;
    private readonly OCSlot<T>[] __Slots;
    private readonly StripeArray __Stripes;
    private readonly RequestBatch<T>[] __Batches = new RequestBatch<T>[2];
    private OCRequest<T>[] __TickWide = [];
    private int __TickWideCount;
    private OCRequest<T>[] __TickThin = [];
    private int __TickThinCount;
    private int __TickThinTakeCounter;

    public ObjectCache(
        ObjectCreateFuncType<T> CreateFunc,
        ObjectDestroyFuncType<T>? DestroyFunc = null,
        int SlotsCount = 256
    )
    {
        __CreateFunc = CreateFunc;
        __DestroyFunc = DestroyFunc;
        __SlotsCount = Math.Max(256, SlotsCount);
        __Slots = new OCSlot<T>[__SlotsCount];
        __Stripes = new StripeArray(
            Math.Min(__SlotsCount, Environment.ProcessorCount)
        );

        for (int Index = 0; Index < __SlotsCount; ++Index)
            __Slots[Index] = new OCSlot<T>();

        for (int Index = 0; Index < __Batches.Length; ++Index)
            __Batches[Index] = new RequestBatch<T>();
    }

    public CacheResult<T> Get(
        Access Acc,
        CacheKeyData Key,
        CacheLookupParams Params,
        long EndTimeUSecs,
        bool IsAsyncThread = false
    )
    {
        int BatchIndex = (Params.Flags & CacheFlags.HighPriority) != 0 ? 0 : 1;
        var Batch = __Batches[BatchIndex];
        ulong KeyHash = Key.Hash;
        ulong SlotIndex = KeyHash % (ulong) __SlotsCount;
        var Slot = __Slots[SlotIndex];
        var Stripe = __Stripes.FromSlot(SlotIndex);
        bool HaveObj = false;
        bool IsStale = true;
        bool RequestNeeded = false;
        T Obj = default;

        using (Stripe.EnterRead())
        {
            for (
                var Node = Slot.Head;
                Node != null;
                Node = Node.Next
            )
            {
                if (Node.Key != Key)
                    continue;

                if (Volatile.Read(ref Node.LastRequestedGeneration) != (long) Params.Generation)
                    Volatile.Write(ref Node.LastRequestedGeneration, (long) Params.Generation);

                bool Stale = Volatile.Read(ref Node.LastCompletedGeneration) != (long) Params.Generation;

                if (
                    Volatile.Read(ref Node.CompletionCount) > 0 &&
                    (!Stale || (Params.Flags & CacheFlags.WaitForFresh) == 0)
                )
                {
                    HaveObj = true;
                    IsStale = Stale;
                    Obj = Node.Value;
                    Acc.TouchPoint(Node.Point);
                }

                if (Stale)
                {
                    long Prev = Interlocked.CompareExchange(ref Node.WorkingCount, 1, 0);

                    RequestNeeded = Prev == 0;
                }

                break;
            }
        }

        if (!HaveObj || RequestNeeded)
        {
            using (Stripe.EnterWrite())
            {
                for (;;)
                {
                    bool OutOfTime = TimeUtil.NowUSecs() >= EndTimeUSecs;
                    OCNode<T>? Node = null;

                    for (
                        var N = Slot.Head;
                        N != null;
                        N = N.Next
                    )
                    {
                        if ( N.Key == Key)
                        {
                            Node = N;
                            break;
                        }
                    }

                    if (Node == null)
                    {
                        RequestNeeded = true;
                        Node = Slot.Free;

                        if (Node != null)
                            Slot.Free = Node.Next;
                        else
                            Node = new OCNode<T>();

                        Node.Reset();
                        DllPushBack(ref Slot.Head, ref Slot.Tail, Node);
                        Node.Key = Key;
                        Node.WorkingCount = 1;
                        Node.EvictionThesholdUSecs = Params.EvictionThresholdUSecs;
                    }

                    Node.Point.LastTimeTouchedUSecs = TimeUtil.NowUSecs();
                    Node.Point.LastUpdateIndexTouched = UpdateTick.Index();

                    if (RequestNeeded)
                    {
                        RequestNeeded = false;
                        Node.Cancel.Reset();

                        bool IsWide = (Params.Flags & CacheFlags.Wide) != 0;

                        Batch.Enqueue(new OCRequest<T>
                        {
                            Key = Key,
                            Generation = Params.Generation,
                            TargetNode = Node,
                            CreateFunc = __CreateFunc,
                            IsWide = IsWide
                        });

                        AsyncLoopSignal.RequestRepeat();

                        if ((Params.Flags & CacheFlags.HighPriority) != 0)
                            AsyncLoopSignal.RequestRepeatHighPriority();
                    }

                    if (
                        !HaveObj &&
                         Node.CompletionCount > 0 &&
                        (
                            Node.LastCompletedGeneration == (long) Params.Generation ||
                            (Params.Flags & CacheFlags.WaitForFresh) == 0 ||
                            OutOfTime
                        )
                    )
                    {
                        HaveObj = true;
                        IsStale =  Node.LastCompletedGeneration != (long) Params.Generation;
                        Obj = Node.Value;
                        Acc.TouchPoint(Node.Point);
                    }

                    if (OutOfTime || HaveObj || IsAsyncThread)
                        break;

                    Stripe.WaitWrite(EndTimeUSecs);
                }
            }
        }

        return new CacheResult<T>(Obj, HaveObj, IsStale);
    }

    void IObjectCache.Evict((int Min, int Max) SlotRange)
    {
        for (
            int Index = SlotRange.Min;
            Index < SlotRange.Max;
            ++Index
        )
        {
            var Slot = __Slots[Index];
            var Stripe = __Stripes.FromSlot((ulong) Index);
            bool HasWork = false;

            using (Stripe.EnterRead())
            {
                for (
                    var Node = Slot.Head;
                    Node != null;
                    Node = Node.Next
                )
                {
                    var Params = new ExpireParams(
                        Volatile.Read(ref Node.EvictionThesholdUSecs)
                    );

                    if (
                        Access.IsExpired(Node.Point, Params) && 
                        Volatile.Read(ref Node.WorkingCount) == 0
                    )
                    {
                        HasWork = true;
                        break;
                    }
                }
            }

            if (!HasWork)
                continue;

            using (Stripe.EnterWrite())
            {
                for (
                    OCNode<T>? Node = Slot.Head, Next = null;
                    Node != null;
                    Node = Next
                )
                {
                    Next = Node.Next;

                    var Params = new ExpireParams(
                        Volatile.Read(ref Node.EvictionThesholdUSecs)
                    );

                    if (
                        Access.IsExpired(Node.Point, Params) &&
                        Volatile.Read(ref Node.WorkingCount) == 0
                    )
                    {
                        DllRemove(ref Slot.Head, ref Slot.Tail, Node);
                        __DestroyFunc?.Invoke(Node.Value);
                        Node.Reset();
                        Node.Next = Slot.Free;
                        Slot.Free = Node;
                    }
                }
            }
        }
    }

    void IObjectCache.CancelExpired()
    {
        for (
            int Index = 0;
            Index < __SlotsCount;
            ++Index
        )
        {
            var Slot = __Slots[Index];
            var Stripe = __Stripes.FromSlot((ulong) Index);
            bool HasWork = false;

            using (Stripe.EnterRead())
            {
                for (
                    var Node = Slot.Head;
                    Node != null;
                    Node = Node.Next
                )
                {
                    var Params = new ExpireParams(
                        Volatile.Read(ref Node.EvictionThesholdUSecs)
                    );

                    if (
                        Access.IsExpired(Node.Point, Params) &&
                        Volatile.Read(ref Node.WorkingCount) > 0
                    )
                    {
                        HasWork = true;
                        break;
                    }
                }
            }

            if (!HasWork)
                continue;

            using (Stripe.EnterWrite())
            {
                for (
                    var Node = Slot.Head;
                    Node != null;
                    Node = Node.Next
                )
                {
                    var Params = new ExpireParams(
                        Volatile.Read(ref Node.EvictionThesholdUSecs)
                    );

                    if (
                        Access.IsExpired(Node.Point, Params) &&
                        Volatile.Read(ref Node.WorkingCount) > 0
                    )
                    {
                        Node.Cancel.Cancel();
                    }
                }
            }
        }
    }

    void IObjectCache.ProcessRequests(LaneGroup? Group)
    {
        int LIndex = Group != null ? LaneGroup.LaneIndex() : 0;
        int LCount = Group != null ? LaneGroup.LaneCount() : 1;

        for (
            int BatchIndex = 0;
            BatchIndex < __Batches.Length;
            ++BatchIndex
        )
        {
            var Batch = __Batches[BatchIndex];

            if (LIndex == 0)
            {
                var Requests = Batch.DrainAll();
                var Wide = new List<OCRequest<T>>();
                var Thin = new List<OCRequest<T>>();

                for (int Index = 0; Index < Requests.Count; ++Index)
                {
                    var Req = Requests[Index];

                    if (Req.IsWide)
                        Wide.Add(Req);
                    else
                        Thin.Add(Req);
                }

                __TickWide = Wide.Count > 0 ? [.. Wide] : [];
                __TickWideCount = Wide.Count;
                __TickThin = Thin.Count > 0 ? [.. Thin] : [];
                __TickThinCount = Thin.Count;
                Volatile.Write(ref __TickThinTakeCounter, 0);
            }

            Group?.Sync();

            int WideCount = Volatile.Read(ref __TickWideCount);
            int ThinCount = Volatile.Read(ref __TickThinCount);
            int CompletedWideCount = 0;
            bool YieldedWide = false;

            for (
                int Index = 0;
                Index < WideCount;
                ++Index
            )
            {
                int Action = 0;

                if (LIndex == 0)
                {
                    var Request = __TickWide[Index];

                    if (Request.TargetNode.Cancel.IsCancelled())
                    {
                        Action = 1;
                    }
                    else if (
                        BatchIndex == 1 &&
                        Index > 0 &&
                        AsyncLoopSignal.IsHighPriorityRepeatRequested()
                    )
                    {
                        Action = 2;
                    }
                }

                if (Group != null)
                    Action = (int) Group.SyncLong(Action, 0);

                if (YieldedWide)
                    continue;

                if (Action == 1)
                {
                    if (LIndex == 0)
                        AbortRequest(__TickWide[Index]);

                    ++CompletedWideCount;
                    continue;
                }

                if (Action == 2)
                {
                    YieldedWide = true;
                    continue;
                }

                var Req = __TickWide[Index];
                bool Retry;
                ulong Generation = Req.Generation;
                T Value = Req.CreateFunc(
                    Req.Key,
                    Req.TargetNode.Cancel,
                    out Retry,
                    ref Generation
                );

                if (LIndex == 0)
                {
                    if (
                        Retry &&
                        !Req.TargetNode.Cancel.IsCancelled()
                    )
                    {
                        Batch.Enqueue(Req);
                        AsyncLoopSignal.RequestRepeat();
                    }
                    else if (Retry)
                    {
                        AbortRequest(Req);
                    }
                    else
                    {
                        CompleteRequest(Req, Value, Generation);
                    }
                }

                ++CompletedWideCount;
            }

            Group?.Sync();

            var LaneRestore = LaneGroup.PushSingleLane();

            for (;;)
            {
                int Completed = Volatile.Read(ref __TickThinTakeCounter);

                if (
                    BatchIndex == 1 &&
                    Completed >= ThinCount / 2 &&
                    AsyncLoopSignal.IsHighPriorityRepeatRequested()
                )
                {
                    break;
                }

                int RequestIndex = Interlocked.Increment(ref __TickThinTakeCounter) - 1;

                if (RequestIndex >= ThinCount)
                    break;

                var Request = __TickThin[RequestIndex];

                if (Request.TargetNode.Cancel.IsCancelled())
                {
                    AbortRequest(Request);
                    continue;
                }

                bool Retry;
                ulong Generation = Request.Generation;
                T Value = Request.CreateFunc(
                    Request.Key,
                    Request.TargetNode.Cancel,
                    out Retry,
                    ref Generation
                );

                if (Retry && !Request.TargetNode.Cancel.IsCancelled())
                {
                    Batch.Enqueue(Request);
                    AsyncLoopSignal.RequestRepeat();
                }
                else if (Retry)
                {
                    AbortRequest(Request);
                }
                else
                {
                    CompleteRequest(Request, Value, Generation);
                }
            }

            LaneGroup.PopLane(LaneRestore);
            Group?.Sync();

            if (LIndex == 0 && BatchIndex > 0)
            {
                int CompletedThinCount = Math.Min(
                    Volatile.Read(ref __TickThinTakeCounter),
                    ThinCount
                );
                bool RetryRequired = (
                    CompletedWideCount < WideCount ||
                    CompletedThinCount < ThinCount
                );

                for (int Index = CompletedWideCount; Index < WideCount; ++Index)
                    Batch.Enqueue(__TickWide[Index]);

                for (int Index = CompletedThinCount; Index < ThinCount; ++Index)
                    Batch.Enqueue(__TickThin[Index]);

                if (RetryRequired)
                    AsyncLoopSignal.RequestRepeat();
            }
        }
    }

    int IObjectCache.SlotCount => __SlotsCount;

    private void CompleteRequest(in OCRequest<T> Request, T Value, ulong Generation)
    {
        var Node = Request.TargetNode;
        var Stripe = __Stripes.FromSlot(Node.Key.Hash % (ulong) __SlotsCount);

        using (Stripe.EnterWrite())
        {
            Node.LastCompletedGeneration = (long) Generation;
            Node.Value = Value;
             --Node.WorkingCount;
            ++Node.CompletionCount;
        }

        Stripe.Broadcast();
    }

    private void AbortRequest(in OCRequest<T> Request)
    {
        var Node = Request.TargetNode;
        var Stripe = __Stripes.FromSlot(Node.Key.Hash % (ulong) __SlotsCount);

        using (Stripe.EnterWrite())
        {
            --Node.WorkingCount;
        }

        Stripe.Broadcast();
    }

    private static void DllPushBack(
        ref OCNode<T>? Head,
        ref OCNode<T>? Tail,
        OCNode<T> Node
    )
    {
        Node.Prev = Tail;
        Node.Next = null;

        if (Tail != null)
            Tail.Next = Node;
        else
            Head = Node;

        Tail = Node;
    }

    private static void DllRemove(
        ref OCNode<T>? Head,
        ref OCNode<T>? Tail,
        OCNode<T> Node
    )
    {
        if (Node.Prev != null)
            Node.Prev.Next = Node.Next;
        else
            Head = Node.Next;

        if (Node.Next != null)
            Node.Next.Prev = Node.Prev;
        else
            Tail = Node.Prev;
    }

    public void Dispose()
    {
        __Stripes.Dispose();
    }
}

internal sealed class RequestBatch<T> where T : struct
{
    private readonly object __Lock = new();
    private List<OCRequest<T>> __Pending = [];

    public void Enqueue(OCRequest<T> Request)
    {
        lock(__Lock)
            __Pending.Add(Request);
    }

    public List<OCRequest<T>> DrainAll()
    {
        lock(__Lock)
        {
            if (__Pending.Count == 0)
                return [];

            var Result = __Pending;

            __Pending = [];

            return Result;
        }
    }
}

public static class AsyncLoopSignal
{
    private static int __Repeat;
    private static int __RepeatHighPriority;

    public static void RequestRepeat()
    {
        Volatile.Write(ref __Repeat, 1);
    }

    public static void RequestRepeatHighPriority()
    {
        Volatile.Write(ref __RepeatHighPriority, 1);
    }

    public static bool IsHighPriorityRepeatRequested()
    {
        return Volatile.Read(ref __RepeatHighPriority) != 0;
    }

    public static bool ConsumeRepeatRequested()
    {
        return Interlocked.Exchange(ref __Repeat, 0) != 0;
    }

    public static bool ConsumeHighPriorityRepeatRequested()
    {
        return Interlocked.Exchange(ref __RepeatHighPriority, 0) != 0;
    }
}

public sealed class ObjectCacheManager : IDisposable, IAsyncDisposable
{
    private readonly List<IObjectCache> __Caches = [];
    private readonly object __CacheLock = new();
    private readonly System.Threading.Tasks.Task __CancelTask;
    private readonly CancellationTokenSource __CTS = new();
    private readonly SemaphoreSlim __CancelSignal = new(0, 1);
    private bool __Disposed;

    public ObjectCacheManager()
    {
#pragma warning disable CS4014
        __CancelTask = System.Threading.Tasks.Task.Run(() => {
            CancelLoopAsync(__CTS.Token); 
        });
#pragma warning restore CS4014
    }

    public void Register(IObjectCache Cache)
    {
        lock(__CacheLock)
            __Caches.Add(Cache);
    }

    public void Tick(LaneGroup? Group = null)
    {
        if (__CancelSignal.CurrentCount == 0)
            try { __CancelSignal.Release(); } catch {}

        IObjectCache[] Snapshot;

        lock(__CacheLock)
            Snapshot = [.. __Caches];

        for (int Index = 0; Index < Snapshot.Length; ++Index)
        {
            var Cache = Snapshot[Index];

            if (Group != null)
            {
                (int Min, int Max) SlotRange = LaneGroup.LaneRange(Cache.SlotCount);

                Cache.Evict(SlotRange);
            }
            else
            {
                (int Min, int Max) SlotRange = (0, Cache.SlotCount);

                Cache.Evict(SlotRange);
            }

            Cache.ProcessRequests(Group);
        }
    }

    private async System.Threading.Tasks.Task CancelLoopAsync(CancellationToken CancelTok)
    {
        try
        {
            while (!CancelTok.IsCancellationRequested)
            {
                await __CancelSignal.WaitAsync(CancelTok);
                await System.Threading.Tasks.Task.Delay(50, CancelTok);

                IObjectCache[] Snapshot;

                lock(__CacheLock)
                    Snapshot = [.. __Caches];

                for (int Index = 0; Index < Snapshot.Length; ++Index)
                    Snapshot[Index].CancelExpired();
            }
        }
        catch (OperationCanceledException)
        {}
        catch (ObjectDisposedException)
        {}
    }

    public async System.Threading.Tasks.ValueTask DisposeAsync()
    {
        if (__Disposed)
            return;

        __Disposed = true;
        await __CTS.CancelAsync();

        try
        {
            await System.Threading.Tasks.Task.WhenAny(
                __CancelTask,
                System.Threading.Tasks.Task.Delay(TimeSpan.FromSeconds(2))
            ).ConfigureAwait(false);
        }
        catch
        {}

        __CTS.Dispose();
        __CancelSignal.Dispose();
    }

    public void Dispose()
    {
        if (__Disposed)
            return;

        __Disposed = true;
        __CTS.Cancel();
        __CTS.Dispose();
        __CancelSignal.Dispose();
    }
}