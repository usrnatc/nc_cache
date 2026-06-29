using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using Microsoft.Win32.SafeHandles;

namespace NcCache;

/// <summary>
/// Shared state for a file read operation, broadcast from Lane0 to all other lanes.
/// 
/// Contains the:
///     - file handle,
///     - buffer, and
///     - metadata
/// needed to coordinate a multi-lane parallel file read.
/// </summary>
internal sealed class ObjectAllocShared
{
    public long ReadSize;
    public byte[]? Buffer;
    public SafeFileHandle? Handle;
    public bool Cancelled;
    public long PrevModifiedTicks;
    public long PrevSize;
}

/// <summary>
/// Result of a file read operation, broadcast from Lane0 to all other lanes.
/// </summary>
internal sealed class ObjectAllocResult
{
    public bool Retry;
    public ContentKey Key;
}

/// <summary>
/// A node in the fast-path lookup table.
/// 
/// Caches a file path to data hash mapping in order to bypass full lookups
/// when we can be sure the file has not changed since last lookup.
/// </summary>
internal sealed class FastPathNode
{
    public FastPathNode? Next;
    public string Path = "";
    public ulong PathHash;
    public ulong Generation;
    public Hash128 DataHash;
}

/// <summary>
/// Hash table bucket for fast path nodes.
/// </summary>
internal sealed class FastPathSlot
{
    public FastPathNode? Head;
}

/// <summary>
/// File caching layer.
/// 
/// Given a file path and a byte range, reads the file,
/// stores the data in <c>ContentCache</c> and returns the content.
/// </summary>
public sealed class FileStreamCache : IDisposable
{
    private readonly ContentCache __Content;
    private readonly ObjectCache<ContentKey> __ObjectCache;
    private readonly int __SlotsCount;
    private readonly FSSlot[] __Slots;
    private readonly FastPathSlot[] __FastSlots;
    private readonly StripeArray __Stripes;
    private long __ChangeGeneration = 1;

    public FileStreamCache(ContentCache Content, int SlotsCount = 1024)
    {
        __Content = Content;
        __SlotsCount = SlotsCount;
        __Slots = new FSSlot[__SlotsCount];
        __FastSlots = new FastPathSlot[__SlotsCount];
        __Stripes = new StripeArray(
            Math.Min(__SlotsCount, Environment.ProcessorCount)
        );

        for (int Index = 0; Index < __SlotsCount; ++Index)
        {
            __Slots[Index] = new FSSlot();
            __FastSlots[Index] = new FastPathSlot();
        }

        __ObjectCache = new ObjectCache<ContentKey>(
            CreateFunc: ObjectAlloc,
            DestroyFunc: ObjectRelease,
            SlotsCount: 256
        );
    }

    public ulong ChangeGeneration()
    {
        return (ulong) Volatile.Read(ref __ChangeGeneration);
    }

    public IObjectCache Cache()
    {
        return __ObjectCache;
    }

    public ContentKey KeyFromPathRange(
        Access Acc,
        string Path, 
        long Offset, 
        long Length, 
        long EndTimeUSecs,
        CacheFlags Flags = CacheFlags.None
    )
    {
        ulong PathHash = Hashing.HashString(Path);

        return KeyFromPathRange(
            Acc, 
            Path, 
            PathHash, 
            Offset, 
            Length, 
            EndTimeUSecs, 
            Flags
        );
    }

    public ContentKey KeyFromPathRange(
        Access Acc,
        string Path, 
        ulong PathHash,
        long Offset, 
        long Length, 
        long EndTimeUSecs,
        CacheFlags Flags = CacheFlags.None
    )
    {
        var CacheKey = new CacheKeyData(Path, Offset, Length, PathHash);
        ulong Generation = GetPathGeneration(Path, PathHash);
        var Params = CacheLookupParams.Default();

        Params.Generation = Generation;
        Params.Flags = Flags;

        var Result = __ObjectCache.Get(Acc, CacheKey, Params, EndTimeUSecs);

        return Result.IsFound ? Result.Value : default;
    }

    public ContentKey KeyFromPath(
        Access Acc,
        string Path,
        long EndTimeUSecs
    )
    {
        long FileSize = new FileInfo(Path).Length;
        ulong PathHash = Hashing.HashString(Path);

        return KeyFromPathRange(
            Acc, 
            Path,
            PathHash,
            0, 
            FileSize > 0 ? FileSize : long.MaxValue,
            EndTimeUSecs
        );
    }

    public Hash128 HashFromPathRange(
        Access Acc,
        string Path,
        long Offset,
        long Length,
        long EndTimeUSecs,
        CacheFlags Flags = CacheFlags.None
    )
    {
        ulong PathHash = Hashing.HashString(Path);
        ContentKey Key = KeyFromPathRange(
            Acc,
            Path,
            PathHash,
            Offset,
            Length,
            EndTimeUSecs,
            Flags
        );

        if (Key == default)
            return Hash128.Zero;

        for (int Rewind = 0; Rewind < KeyNode.HashHistoryCount; ++Rewind)
        {
            Hash128 Hash = __Content.HashFromKey(Key, (ulong) Rewind);

            if (Hash != Hash128.Zero)
                return Hash;
        }

        return Hash128.Zero;
    }

    public ReadOnlyMemory<byte> DataFromHash(
        Access Acc,
        Hash128 Hash
    )
    {
        return __Content.DataFromHash(Acc, Hash);
    }

    public ReadOnlyMemory<byte> ReadFile(
        Access Acc,
        string Path,
        long EndTimeUSecs,
        CacheFlags Flags = CacheFlags.None
    )
    {
        ulong PathHash = Hashing.HashString(Path);
        ulong SlotIndex = PathHash % (ulong) __SlotsCount;
        var Stripe = __Stripes.FromSlot(SlotIndex);

        if (Flags == CacheFlags.None)
        {
            Hash128 FoundHash = default;

            using (Stripe.EnterRead())
            {
                Hash128 CachedHash = default;
                ulong CachedGeneration = 0;
                bool FoundFastSlot = false;

                for (
                    var FastSlot = __FastSlots[SlotIndex].Head;
                    FastSlot != null;
                    FastSlot = FastSlot.Next
                )
                {
                    if (FastSlot.PathHash == PathHash && FastSlot.Path == Path)
                    {
                        CachedHash = FastSlot.DataHash;
                        CachedGeneration = FastSlot.Generation;
                        FoundFastSlot = true;
                        break;
                    }
                }

                if (FoundFastSlot && CachedHash != Hash128.Zero)
                {
                    for (
                        var Node = __Slots[SlotIndex].Head;
                        Node != null;
                        Node = Node.Next
                    )
                    {
                        if (Node.PathHash == PathHash && Node.Path == Path)
                        {
                            if (CachedGeneration == Node.Generation)
                            {
                                FoundHash = CachedHash;
                            }

                            break;
                        }
                    }
                }
            }

            if (FoundHash != Hash128.Zero)
                return __Content.DataFromHash(Acc, FoundHash);
        }

        ContentKey Key = KeyFromPathRange(
            Acc,
            Path,
            PathHash,
            0,
            long.MaxValue,
            EndTimeUSecs,
            Flags
        );

        if (Key == default)
            return ReadOnlyMemory<byte>.Empty;

        Hash128 Hash = Hash128.Zero;

        for (
            int Rewind = 0;
            Rewind < KeyNode.HashHistoryCount;
            ++Rewind
        )
        {
            Hash = __Content.HashFromKey(Key, (ulong) Rewind);

            if (Hash != Hash128.Zero)
                break;
        }

        if (Hash == Hash128.Zero)
            return ReadOnlyMemory<byte>.Empty;

        if (Flags == CacheFlags.None)
            FastPathUpdate(Path, PathHash, SlotIndex, Stripe, Hash);

        return __Content.DataFromHash(Acc, Hash);
    }

    private void FastPathUpdate(
        string Path,
        ulong PathHash,
        ulong SlotIndex,
        Stripe Stripe,
        Hash128 DataHash
    )
    {
        Hash128 PrevHash = default;

        using (Stripe.EnterWrite())
        {
            ulong Generation = 0;

            for (
                var N = __Slots[SlotIndex].Head;
                N != null;
                N = N.Next
            )
            {
                if (N.PathHash == PathHash && N.Path == Path)
                {
                    Generation = N.Generation;
                    break;
                }
            }

            FastPathNode? Node = null;

            for (
                var FastNode = __FastSlots[SlotIndex].Head;
                FastNode != null;
                FastNode = FastNode.Next
            )
            {
                if (FastNode.PathHash == PathHash && FastNode.Path == Path)
                {
                    Node = FastNode;
                    break;
                }
            }

            if (Node == null)
            {
                Node = new FastPathNode
                {
                    Path = Path,
                    PathHash = PathHash,
                    Next = __FastSlots[SlotIndex].Head
                };
                __FastSlots[SlotIndex].Head = Node;
            }

            PrevHash = Node.DataHash;
            Node.Generation = Generation;
            Node.DataHash = DataHash;
        }

        if (DataHash != Hash128.Zero)
            __Content.HashDownstreamInc(DataHash);

        if (PrevHash != Hash128.Zero && PrevHash != DataHash)
            __Content.HashDownstreamDec(PrevHash);
    }

    private static Hash128 FastPathInvalidate(
        FastPathSlot Slot,
        ulong PathHash,
        string Path
    )
    {
        for (
            var Node = Slot.Head;
            Node != null;
            Node = Node.Next
        )
        {
            if (Node.PathHash == PathHash && Node.Path == Path)
            {
                Hash128 PrevHash = Node.DataHash;

                Node.DataHash = default;
                Node.Generation = 0;

                return PrevHash;
            }
        }

        return default;
    }

    public void AsyncTick(LaneGroup? Group)
    {
        var (Min, Max) = Group != null
            ? LaneGroup.LaneRange(__SlotsCount)
            : (0, __SlotsCount);

        AsyncTick(Min, Max);
    }

    public void AsyncTick(int SlotMin, int SlotMax)
    {
        for (int SlotIndex = SlotMin; SlotIndex < SlotMax; ++SlotIndex)
        {
            var Slot = __Slots[SlotIndex];
            var Stripe = __Stripes.FromSlot((ulong) SlotIndex);
            bool HasWork = false;

            using (Stripe.EnterRead())
            {
                for (var Node = Slot.Head; Node != null; Node = Node.Next)
                {
                    long LastWriteTimeTicks = File.GetLastWriteTimeUtc(Node.Path).Ticks;
                    if (
                        LastWriteTimeTicks != Node.LastModifiedTicks
                    )
                    {
                        HasWork = true;
                        break;
                    }
                }
            }

            if (!HasWork)
                continue;

            Hash128[] InvalidatedHashes = [];
            int InvalidatedHashesCount = 0;

            using (Stripe.EnterWrite())
            {
                for (var Node = Slot.Head; Node != null; Node = Node.Next)
                {
                    var Info = new FileInfo(Node.Path);

                    if (
                        Info.Exists &&
                        Info.LastWriteTimeUtc.Ticks != Node.LastModifiedTicks
                    )
                    {
                        ++Node.Generation;
                        Node.LastModifiedTicks = Info.LastWriteTimeUtc.Ticks;
                        Interlocked.Increment(ref __ChangeGeneration);

                        Hash128 PrevHash = FastPathInvalidate(
                            __FastSlots[SlotIndex],
                            Node.PathHash,
                            Node.Path
                        );

                        if (PrevHash != Hash128.Zero)
                        {
                            if (InvalidatedHashesCount >= InvalidatedHashes.Length)
                                Array.Resize(ref InvalidatedHashes, Math.Max(4, InvalidatedHashes.Length * 2));

                            InvalidatedHashes[InvalidatedHashesCount++] = PrevHash;
                        }
                    }
                }
            }

            for (int Index = 0; Index < InvalidatedHashesCount; ++Index)
                __Content.HashDownstreamDec(InvalidatedHashes[Index]);
        }
    }

    public void AsyncTick()
    {
        AsyncTick(0, __SlotsCount);
    }

    private ContentKey ObjectAlloc(
        CacheKeyData Key,
        CancelToken Cancel,
        out bool Retry,
        ref ulong Generation
    )
    {
        Retry = false;

        string Path = Key.Path;
        long Offset = Key.Offset;
        long Length = Key.Length;
        var Group = LaneGroup.Current();
        int LIndex = LaneGroup.LaneIndex();
        int LCount = LaneGroup.LaneCount();
        bool IsMultiLane = Group != null && LCount > 1;
        ObjectAllocShared? AllocShared = null;

        if (LIndex == 0)
        {
            AllocShared = new ObjectAllocShared();

            try
            {
                var PrevInfo = new FileInfo(Path);

                if (PrevInfo.Exists)
                {
                    AllocShared.PrevModifiedTicks = PrevInfo.LastWriteTimeUtc.Ticks;
                    AllocShared.PrevSize = PrevInfo.Length;
                    AllocShared.ReadSize = Math.Min(AllocShared.PrevSize - Offset, Length);

                    if (AllocShared.ReadSize <= 0)
                        AllocShared.ReadSize = 0;
                }
            }
            catch
            {
                AllocShared.ReadSize = 0;
                Retry = true;
            }

            if (AllocShared.ReadSize > 0 && !Cancel.IsCancelled())
            {
                AllocShared.Buffer = ArrayPool<byte>.Shared.Rent((int) AllocShared.ReadSize);
                AllocShared.Handle = File.OpenHandle(
                    Path, 
                    FileMode.Open, 
                    FileAccess.Read, 
                    FileShare.ReadWrite
                );

                if (AllocShared.Handle == null || AllocShared.Handle.IsInvalid)
                {
                    ArrayPool<byte>.Shared.Return(AllocShared.Buffer);
                    AllocShared.Buffer = null;
                    AllocShared.Handle?.Dispose();
                    AllocShared.Handle = null;
                    AllocShared.ReadSize = 0;
                    Retry = true;
                }
            }
            else if (AllocShared.ReadSize > 0)
            {
                AllocShared.Cancelled = true;
                AllocShared.ReadSize = 0;
            }

            if (IsMultiLane)
                Group!.ResetAccumulator();
        }

        if (IsMultiLane)
            AllocShared = Group!.SyncObj(AllocShared, 0);

        if (AllocShared!.Cancelled)
        {
            Retry = true;

            return default;
        }

        if (AllocShared.ReadSize == 0)
            return default;

        long ReadSize = AllocShared.ReadSize;
        byte[] Buffer = AllocShared.Buffer!;
        SafeFileHandle Handle = AllocShared.Handle!;
        long LaneReadStart;
        long LaneReadEnd;

        if (IsMultiLane)
        {
            (LaneReadStart, LaneReadEnd) = LaneGroup.LaneRange(ReadSize);
        }
        else
        {
            LaneReadStart = 0;
            LaneReadEnd = ReadSize;
        }

        int LaneReadSize = (int) (LaneReadEnd - LaneReadStart);
        long LaneBytesRead = 0;

        if (LaneReadSize > 0)
        {
            try
            {
                LaneBytesRead = RandomAccess.Read(
                    Handle,
                    Buffer.AsSpan((int) LaneReadStart, LaneReadSize),
                    Offset + LaneReadStart
                );
            }
            catch
            {
                LaneBytesRead = 0;
            }
        }

        long TotalRead;

        if (IsMultiLane)
        {
            Group!.AccumulateAdd(LaneBytesRead);
            TotalRead = Group.SyncAndReadAccumulator();
        }
        else
        {
            TotalRead = LaneBytesRead;
        }

        ObjectAllocResult? Result = null;

        if (LIndex == 0)
        {
            Handle.Dispose();
            Result = new ObjectAllocResult();

            bool ReadOkay;

            try
            {
                var PostInfo = new FileInfo(Path);

                ReadOkay = (
                    PostInfo.LastWriteTimeUtc.Ticks == AllocShared.PrevModifiedTicks &&
                    PostInfo.Length == AllocShared.PrevSize &&
                    TotalRead == ReadSize
                );
            }
            catch
            {
                ReadOkay = false;
            }

            if (!ReadOkay)
            {
                ArrayPool<byte>.Shared.Return(Buffer);
                Result.Retry = true;
            }
            else
            {
                Hash128 KeyHash = ComputeContentIDHash(Key);
                var ID = new ContentID(KeyHash.Lo, KeyHash.Hi);
                
                Result.Key = new ContentKey(default, ID);
                __Content.SubmitData(Result.Key, Buffer!, (int) ReadSize);
                UpdateFileTracking(Path, Key.PathHash, AllocShared.PrevModifiedTicks, AllocShared.PrevSize);
            }
        }

        if (IsMultiLane)
            Result = Group!.SyncObj(Result, 0);

        Retry = Result!.Retry;

        return Result.Key;
    }

    private static Hash128 ComputeContentIDHash(in CacheKeyData Key)
    {
        Span<byte> Buf = stackalloc byte[24];

        BinaryPrimitives.WriteUInt64LittleEndian(Buf, Key.PathHash);
        BinaryPrimitives.WriteInt64LittleEndian(Buf[8 ..], Key.Offset);
        BinaryPrimitives.WriteInt64LittleEndian(Buf[16 ..], Key.Length);

        return Hashing.Hash128(Buf);
    }

    private void ObjectRelease(ContentKey Key)
    {
        if (Key != default)
            __Content.CloseKey(Key);
    }

    private ulong GetPathGeneration(string Path, ulong PathHash)
    {
        ulong SlotIndex = PathHash % (ulong) __SlotsCount;
        var Slot = __Slots[SlotIndex];
        var Stripe = __Stripes.FromSlot(SlotIndex);

        using (Stripe.EnterRead())
        {
            for (var Node = Slot.Head; Node != null; Node = Node.Next)
            {
                if (Node.PathHash == PathHash && Node.Path == Path)
                    return Node.Generation;
            }
        }

        return 0;
    }

    private void UpdateFileTracking(string Path, ulong PathHash, long ModifiedTicks, long Size)
    {
        ulong SlotIndex = PathHash % (ulong) __SlotsCount;
        var Slot = __Slots[SlotIndex];
        var Stripe = __Stripes.FromSlot(SlotIndex);

        using (Stripe.EnterWrite())
        {
            FSNode? Node = null;

            for (
                var N = Slot.Head;
                N != null;
                N = N.Next
            )
            {
                if (N.PathHash == PathHash && N.Path == Path)
                {
                    Node = N;
                    break;
                }
            }

            if (Node == null)
            {
                Node = new FSNode
                {
                    Path = Path,
                    PathHash = PathHash
                };

                if (Slot.Tail != null)
                    Slot.Tail.Next = Node;
                else
                    Slot.Head = Node;

                Slot.Tail = Node;
            }

            Node.LastModifiedTicks = ModifiedTicks;
            Node.Size = Size;
        }
    }

    public void Dispose()
    {
        for (int SlotIndex = 0; SlotIndex < __SlotsCount; ++SlotIndex)
        {
            for (
                var Node = __FastSlots[SlotIndex].Head;
                Node != null;
                Node = Node.Next
            )
            {
                if (Node.DataHash != Hash128.Zero)
                {
                    __Content.HashDownstreamDec(Node.DataHash);
                    Node.DataHash = default;
                }
            }
        }

        __Stripes.Dispose();
    }
}

internal sealed class FSNode
{
    public FSNode? Next;
    public string Path = "";
    public ulong PathHash;
    public ulong Generation;
    public long LastModifiedTicks;
    public long Size;
}

internal sealed class FSSlot
{
    public FSNode? Head;
    public FSNode? Tail;
}