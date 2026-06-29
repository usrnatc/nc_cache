using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;

namespace NcCache;

/// <summary>
/// A <c>ContentRoot</c> is like a namespace for cached data.
/// It is essentially just a group of cache keys.
/// When a <c>ContentRoot</c> is released, all keys registered under it are
/// also released.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct ContentRoot : IEquatable<ContentRoot>
{
    public readonly ulong Value;

    public ContentRoot(ulong Value)
    {
        this.Value = Value;
    }

    public bool IsZero() => Value == 0;

    public bool Equals(ContentRoot Other) => Value == Other.Value;

    public override bool Equals(object? Obj) => Obj is ContentRoot O && Equals(O);

    public override int GetHashCode() => Value.GetHashCode();

    public static bool operator==(ContentRoot ValueA, ContentRoot ValueB) => ValueA.Value == ValueB.Value;

    public static bool operator!=(ContentRoot ValueA, ContentRoot ValueB) => ValueA.Value != ValueB.Value;
}

/// <summary>
/// A <c>ContentID</c> is a 128-bit content identifier (duh)
/// derived from a content key hash.
/// 
/// Paired with a <c>ContentRoot</c> forms a <c>ContentKey</c>.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct ContentID : IEquatable<ContentID>
{
    public readonly ulong V0;
    public readonly ulong V1;

    public ContentID(ulong V0, ulong V1)
    {
        this.V0 = V0;
        this.V1 = V1;
    }

    public bool Equals(ContentID Other) => V0 == Other.V0 && V1 == Other.V1;
    
    public override bool Equals(object? Obj) => Obj is ContentID O && Equals(O);

    public override int GetHashCode() => HashCode.Combine(V0, V1);

    public static bool operator==(ContentID ValueA, ContentID ValueB) => ValueA.V0 == ValueB.V0 && ValueA.V1 == ValueB.V1;
    public static bool operator!=(ContentID ValueA, ContentID ValueB) => ValueA.V0 != ValueB.V0 || ValueA.V1 != ValueB.V1;
}

/// <summary>
/// A <c>ContentKey</c> is a composite key for cached data blobs.
/// Consists of a <c>ContentRoot</c> and a <c>ContentID</c>.
/// 
/// The <c>ContentRoot</c> allows bulk cleanup on session end and 
/// the <c>ContentID</c> identifies specific data blobs within the <c>ContentRoot</c>.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct ContentKey : IEquatable<ContentKey>
{
    public readonly ContentRoot Root;
    public readonly ContentID ID;

    public ContentKey(ContentRoot Root, ContentID ID)
    {
        this.Root = Root;
        this.ID = ID;
    }

    public bool Equals(ContentKey Other) => Root == Other.Root && ID == Other.ID;
    
    public override bool Equals(object? Obj) => Obj is ContentKey O && Equals(O);

    public override int GetHashCode() => HashCode.Combine(Root, ID);

    public static bool operator==(ContentKey ValueA, ContentKey ValueB) => ValueA.Root == ValueB.Root && ValueA.ID == ValueB.ID;
    public static bool operator!=(ContentKey ValueA, ContentKey ValueB) => ValueA.Root != ValueB.Root || ValueA.ID != ValueB.ID;
}

/// <summary>
/// A <c>BlobNode</c> represents a node in the Blob hash table and stores
/// the actual cached data blob.
/// 
/// Acts as an intrusive doubly-linked list for use in a <c>BlobSlot</c>.
/// 
///     - KeyReferenceCount is the number of <c>KeyNode</c>s that point to this
///       Blob via hash history.
///     - DownstreamReferenceCount ensures Blobs held in fast paths and/or by
///       external consumers are not evicted whilst they are being used.
/// </summary>
internal sealed class BlobNode
{
    public BlobNode?   Next;
    public BlobNode?   Prev;
    public Hash128     BlobHash;
    public byte[]?     Data;
    public int         DataLength;
    public AccessPoint Point = new();
    public long        KeyReferenceCount;
    public long        DownstreamReferenceCount;

    /// <summary>
    /// Clear all fields and return <c>Data</c> to the shared array pool.
    /// </summary>
    public void Reset()
    {
        Next = null;
        Prev = null;
        BlobHash = default;

        if (Data != null)
        {
            ArrayPool<byte>.Shared.Return(Data);
            Data = null;
        }

        DataLength = 0;
        Point = new AccessPoint();
        KeyReferenceCount = 0;
        DownstreamReferenceCount = 0;
    }
}

/// <summary>
/// A <c>BlobSlot</c> is a hash table bucket for cached Data Blobs.
/// </summary>
internal sealed class BlobSlot
{
    public BlobNode? Head;
    public BlobNode? Tail;
}

/// <summary>
/// A <c>KeyNode</c> is an actual node in the hash table.
/// 
/// Maps a <c>ContentKey</c> to a rolling history of hashes of a specific key
/// over time.
/// 
/// The has history is a ring buffer of <c>HashHistoryCount</c> hashes, with only
/// <c>HashHistoryStrongRefCount</c> of them actually keeping the Blob's
/// <c>KeyReferenceCount</c> incremented.
/// The older hashes in the history are useful for rewinding data to previous
/// versions if the latest version is not yet available.
/// </summary>
internal sealed class KeyNode
{
    public const int HashHistoryCount = 64;
    public const int HashHistoryStrongRefCount = 2;

    public KeyNode? Next;
    public KeyNode? Prev;
    public ContentKey Key;
    public Hash128[] HashHistory = new Hash128[HashHistoryCount];
    public ulong HashHistoryGeneration;

    public void Reset()
    {
        Next = null;
        Prev = null;
        Key = default;
        Array.Clear(HashHistory);
        HashHistoryGeneration = 0;
    }
}

/// <summary>
/// A <c>KeySlot</c> is a hash table bucket for key nodes.
/// </summary>
internal sealed class KeySlot
{
    public KeyNode? Head;
    public KeyNode? Tail;
}

/// <summary>
/// A <c>RootNode</c> is a node in the Root hash table.
/// 
/// Tracks <c>ContentID</c>s associated with a <c>ContentRoot</c> allowing batched
/// closing when the <c>ContentRoot</c> is released.
/// </summary>
internal sealed class RootNode
{
    public RootNode? Next;
    public RootNode? Prev;
    public ContentRoot Root;

    // NOTE(nathan): ContentIDs registered under this ContentRoot
    public List<ContentID> IDs = [];

    public void Reset()
    {
        Next = null;
        Prev = null;
        Root = default;
        IDs.Clear();
    }
}

/// <summary>
/// A <c>RootSlot</c> is a hash table bucket for root nodes.
/// </summary>
internal sealed class RootSlot
{
    public RootNode? Head;
    public RootNode? Tail;
}

/// <summary>
/// A <c>ContentCache</c> is a content-addressable, thread-safe data store.
/// 
/// Three hash tables:
///     - Blob Table
///         - Maps data hashes to data
/// 
///     - Key Table:
///         - Maps <c>ContentKey</c>s to rolling history of hashes.
/// 
///     - Root Table:
///         - Maps <c>ContentRoot</c> to a list of <c>ContentID</c>s.
/// 
/// All tables use striped locking to reduce contention.
/// Eviction of Data Blobs is time-based + reference-counting.
/// </summary>
public sealed class ContentCache : IDisposable
{
    private readonly int         __BlobSlotsCount;
    private readonly int         __KeySlotsCount;
    private readonly int         __RootSlotsCount;
    private readonly BlobSlot[]  __BlobSlots;
    private readonly StripeArray __BlobStripes;
    private readonly BlobNode?[] __BlobFreeNodes;
    private readonly KeySlot[]   __KeySlots;
    private readonly StripeArray __KeyStripes;
    private readonly KeyNode?[]  __KeyFreeNodes;
    private readonly RootSlot[]  __RootSlots;
    private readonly StripeArray __RootStripes;
    private readonly RootNode?[] __RootFreeNodes;
    private long                 __RootIDGeneration;
    private readonly long        __BlobEvictionTimeUSecs;

    public ContentCache(
        int    BlobSlotsCount       = 16384,
        int    KeySlotsCount        = 4096,
        int    RootSlotsCount       = 4096,
        double BlobEvictionTimeSecs = 5.0
    )
    {
        int CPUCount = Environment.ProcessorCount;

        // init blob table
        __BlobSlotsCount = BlobSlotsCount;
        __BlobSlots = CreateSlots<BlobSlot>(__BlobSlotsCount);
        __BlobStripes = new StripeArray(Math.Min(__BlobSlotsCount, CPUCount));
        __BlobFreeNodes = new BlobNode[__BlobStripes.Count];

        // init key table
        __KeySlotsCount = KeySlotsCount;
        __KeySlots = CreateSlots<KeySlot>(__KeySlotsCount);
        __KeyStripes = new StripeArray(Math.Min(__KeySlotsCount, CPUCount));
        __KeyFreeNodes = new KeyNode[__KeyStripes.Count];

        // init root table
        __RootSlotsCount = RootSlotsCount;
        __RootSlots = CreateSlots<RootSlot>(__RootSlotsCount);
        __RootStripes = new StripeArray(Math.Min(__RootSlotsCount, CPUCount));
        __RootFreeNodes = new RootNode[__RootStripes.Count];

        __BlobEvictionTimeUSecs = TimeUtil.USecsFromSecs(BlobEvictionTimeSecs);
    }

    private static T[] CreateSlots<T>(int Count) where T : new()
    {
        var Result = new T[Count];

        for (int Index = 0; Index < Count; ++Index)
            Result[Index] = new T();

        return Result;
    }

    /// <summary>
    /// Allocate a new Root and insert it into the Root Table.
    /// 
    /// <c>RootRelease()</c> must eventually be called to release all keys associated
    /// with this Root.
    /// </summary>
    /// <returns>Handle to new Root</returns>
    public ContentRoot RootAlloc()
    {
        ulong ID = (ulong)Interlocked.Increment(ref __RootIDGeneration);
        var Root = new ContentRoot(ID);
        ulong SlotIndex = ID % (ulong)__RootSlotsCount;
        int StripeIndex = __RootStripes.StripeIndex(SlotIndex);
        var Slot = __RootSlots[SlotIndex];
        var Stripe = __RootStripes.FromSlot(SlotIndex);

        using (Stripe.EnterWrite())
        {
            // NOTE(nathan): try and use existing node instead of allocating
            RootNode? Node = __RootFreeNodes[StripeIndex];

            if (Node != null)
                __RootFreeNodes[StripeIndex] = Node.Next;
            else
                Node = new RootNode();

            Node.Reset();
            Node.Root = Root;
            DllPushBack(ref Slot.Head, ref Slot.Tail, Node);
        }

        return Root;
    }

    /// <summary>
    /// Release a Root, closing all keys associated with it.
    /// 
    /// The Root is removed from the Root Table, and associated references counts
    /// are decremented.
    /// </summary>
    public void RootRelease(ContentRoot Root)
    {
        ulong SlotIndex = Root.Value % (ulong)__RootSlotsCount;
        int StripeIndex = __RootStripes.StripeIndex(SlotIndex);
        var Slot = __RootSlots[SlotIndex];
        var Stripe = __RootStripes.FromSlot(SlotIndex);
        List<ContentID>? IDs = null;

        using (Stripe.EnterWrite())
        {
            // linear scan for root node
            for (var Node = Slot.Head; Node != null; Node = Node.Next)
            {
                // found, copy its associated keys
                if (Node.Root == Root)
                {
                    DllRemove(ref Slot.Head, ref Slot.Tail, Node);
                    IDs = [.. Node.IDs];
                    Node.Reset();
                    Node.Next = __RootFreeNodes[StripeIndex];
                    __RootFreeNodes[StripeIndex] = Node;
                    break;
                }
            }
        }

        // close associated keys
        if (IDs != null)
        {
            for (int Index = 0; Index < IDs.Count; ++Index)
                CloseKey(new ContentKey(Root, IDs[Index]));
        }
    }

    /// <summary>
    /// Submit data for a given <c>ContentKey</c>.
    /// 
    /// Hashes the data, stores or de-duplicates the Data Blob, and updates
    /// key history.
    /// 
    ///     - hash data to get blob hash
    ///     - find or create blob node for data hash
    ///     - append blob hash to key history
    ///     - if we caused a hash to fall outside of the strong reference window,
    ///       decrement its blob reference count
    ///     - if the key was new, register it under corresponding Root.
    /// 
    /// Caller passes ownership of Data buffer, if existing data is found to be a
    /// match, the Data buffer is cleared and returned to array pool.
    /// </summary>
    public Hash128 SubmitData(ContentKey Key, byte[] Data, int DataLength)
    {
        Hash128 DataHash = Hashing.Hash128(Data.AsSpan(0, DataLength));

        ulong BlobSlotIndex = DataHash.Hi % (ulong)__BlobSlotsCount;
        int BlobStripeIndex = __BlobStripes.StripeIndex(BlobSlotIndex);
        var BlobSlot = __BlobSlots[BlobSlotIndex];
        var BlobStripe = __BlobStripes.FromSlot(BlobSlotIndex);

        using (BlobStripe.EnterWrite())
        {
            BlobNode? Node = null;

            // linear search for blob node
            for (var N = BlobSlot.Head; N != null; N = N.Next)
            {
                // found
                if (N.BlobHash == DataHash)
                {
                    Node = N;
                    break;
                }
            }

            if (Node == null)
            {
                // try and use free blob node
                Node = __BlobFreeNodes[BlobStripeIndex];

                if (Node != null)
                    __BlobFreeNodes[BlobStripeIndex] = Node.Next;
                else
                    Node = new BlobNode();

                Node.Reset();
                Node.BlobHash = DataHash;
                Node.Data = Data;
                Node.DataLength = DataLength;
                DllPushBack(ref BlobSlot.Head, ref BlobSlot.Tail, Node);
            } 
            else
            {
                // data already existed, this buffer is not needed
                ArrayPool<byte>.Shared.Return(Data);
            }

            ++Node.KeyReferenceCount;
        }

        // update hash key history
        ulong KeyHash = Hashing.HashStruct(Key);
        ulong KeySlotIndex = KeyHash % (ulong)__KeySlotsCount;
        int KeyStripeIndex = __KeyStripes.StripeIndex(KeySlotIndex);
        var KeySlot = __KeySlots[KeySlotIndex];
        var KeyStripe = __KeyStripes.FromSlot(KeySlotIndex);
        Hash128 ExpiredHash = default;

        using (KeyStripe.EnterWrite())
        {
            KeyNode? Node = null;

            for (var N = KeySlot.Head; N != null; N = N.Next)
            {
                if (N.Key == Key) 
                {
                    Node = N;
                    break;
                }
            }

            bool KeyIsNew = false;

            if (Node == null)
            {
                KeyIsNew = true;
                Node = __KeyFreeNodes[KeyStripeIndex];

                if (Node != null)
                    __KeyFreeNodes[KeyStripeIndex] = Node.Next;
                else
                    Node = new KeyNode();

                Node.Reset();
                Node.Key = Key;
                DllPushBack(ref KeySlot.Head, ref KeySlot.Tail, Node);
            }

            // check if we pushed an older key out of strong reference counting
            if (Node.HashHistoryGeneration >= KeyNode.HashHistoryStrongRefCount)
            {
                ulong EvictionIndex = Node.HashHistoryGeneration - KeyNode.HashHistoryStrongRefCount;

                ExpiredHash = Node.HashHistory[EvictionIndex % KeyNode.HashHistoryCount];
            }

            Node.HashHistory[Node.HashHistoryGeneration % KeyNode.HashHistoryCount] = DataHash;
            ++Node.HashHistoryGeneration;

            // if key was new, must register it under corresponding root
            if (KeyIsNew)
                RegisterKeyUnderRoot(Key);
        }

        if (ExpiredHash != Hash128.Zero)
            DecrementBlobKeyRef(ExpiredHash);

        return DataHash;
    }

    /// <summary>
    /// Close a <c>ContentKey</c>.
    /// 
    /// Remove the key from the Key Table and decrement blob reference counts for
    /// all hashes in its strong reference counting window.
    /// 
    /// Called when a root is released or when a key is no longer needed.
    /// </summary>
    public void CloseKey(ContentKey Key)
    {
        ulong KeyHash = Hashing.HashStruct(Key);
        ulong KeySlotIndex = KeyHash % (ulong) __KeySlotsCount;
        int KeyStripeIndex = __KeyStripes.StripeIndex(KeySlotIndex);
        var Slot = __KeySlots[KeySlotIndex];
        var Stripe = __KeyStripes.FromSlot(KeySlotIndex);

        Hash128[] ToDecrement = [];
        int ToDecrementCount = 0;

        using (Stripe.EnterWrite())
        {
            for (var Node = Slot.Head; Node != null; Node = Node.Next)
            {
                if (Node.Key != Key)
                    continue;

                int StrongCount = (int) Math.Min(
                    KeyNode.HashHistoryStrongRefCount,
                    Node.HashHistoryGeneration
                );

                ToDecrement = new Hash128[StrongCount];

                // collect all strongly reference counted hashes
                for (int Index = 0; Index < StrongCount; ++Index)
                {
                    ulong HashIndex = Node.HashHistoryGeneration - 1 - (ulong) Index;

                    ToDecrement[ToDecrementCount++] = Node.HashHistory[HashIndex % KeyNode.HashHistoryCount];
                }

                DllRemove(ref Slot.Head, ref Slot.Tail, Node);
                Node.Reset();
                Node.Next = __KeyFreeNodes[KeyStripeIndex];
                __KeyFreeNodes[KeyStripeIndex] = Node;
                break;
            }
        }

        for (int Index = 0; Index < ToDecrementCount; ++Index)
            DecrementBlobKeyRef(ToDecrement[Index]);
    }

    /// <summary>
    /// Incremenet the downstream reference count for a blob.
    /// 
    /// Used to keep a blob alive when being used in a fast path or by 
    /// an external consumer.
    /// </summary>
    public void HashDownstreamInc(Hash128 Hash)
    {
        ulong SlotIndex = Hash.Hi % (ulong) __BlobSlotsCount;
        var Slot = __BlobSlots[SlotIndex];
        var Stripe = __BlobStripes.FromSlot(SlotIndex);

        using (Stripe.EnterRead())
        {
            for (var Node = Slot.Head; Node != null; Node = Node.Next) 
            {
                if (Node.BlobHash == Hash) 
                {
                    Interlocked.Increment(ref Node.DownstreamReferenceCount);
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Decrement the downstream reference count for a blob.
    /// 
    /// Used to signal a blob is no longer needed in a fast path or by an
    /// external consumer.
    /// </summary>
    public void HashDownstreamDec(Hash128 Hash)
    {
        ulong SlotIndex = Hash.Hi % (ulong) __BlobSlotsCount;
        var Slot = __BlobSlots[SlotIndex];
        var Stripe = __BlobStripes.FromSlot(SlotIndex);

        using (Stripe.EnterRead())
        {
            for (var Node = Slot.Head; Node != null; Node = Node.Next) 
            {
                if (Node.BlobHash == Hash) 
                {
                    Interlocked.Decrement(ref Node.DownstreamReferenceCount);
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Look up the Data Hash for a given <c>ContentKey</c>, optionally rewinding
    /// its history for older versions of the Data Hash.
    /// 
    /// RewindCount = 0 retrieves most up-to-data data hash.
    /// 
    /// Returns Hash128.Zero if key does not exist or rewind was too large.
    /// </summary>
    public Hash128 HashFromKey(ContentKey Key, ulong RewindCount = 0)
    {
        ulong KeyHash = Hashing.HashStruct(Key);
        ulong SlotIndex = KeyHash % (ulong) __KeySlotsCount;
        var Slot = __KeySlots[SlotIndex];
        var Stripe = __KeyStripes.FromSlot(SlotIndex);

        using (Stripe.EnterRead())
        {
            for (var Node = Slot.Head; Node != null; Node = Node.Next)
            {
                if (Node.Key == Key) {
                    if (
                        Node.HashHistoryGeneration > 0 &&
                        Node.HashHistoryGeneration - 1 >= RewindCount
                    )
                    {
                        ulong HashIndex = Node.HashHistoryGeneration - 1 - RewindCount;

                        return Node.HashHistory[HashIndex % KeyNode.HashHistoryCount];
                    }
                }
            }
        }

        return Hash128.Zero;
    }

    /// <summary>
    /// Retrieve the raw byte data for a given hash.
    /// 
    /// Touches the blob's <c>AccessPoint</c> to prevent eviction.
    /// 
    /// If the hash is not found, returns empty memory.
    /// </summary>
    public ReadOnlyMemory<byte> DataFromHash(Access Acc, Hash128 Hash)
    {
        ulong SlotIndex = Hash.Hi % (ulong) __BlobSlotsCount;
        var Slot = __BlobSlots[SlotIndex];
        var Stripe = __BlobStripes.FromSlot(SlotIndex);

        using (Stripe.EnterRead())
        {
            for (var Node = Slot.Head; Node != null; Node = Node.Next)
            {
                if (Node.BlobHash == Hash)
                {
                    Acc.TouchPoint(Node.Point);

                    return Node.Data != null
                        ? Node.Data.AsMemory(0, Node.DataLength)
                        : ReadOnlyMemory<byte>.Empty;
                }
            }
        }

        return ReadOnlyMemory<byte>.Empty;
    }

    /// <summary>
    /// Run eviction over the Blob Table for a given slot range.
    /// </summary>
    public void AsyncTick(LaneGroup? Group)
    {
        var (Min, Max) = Group != null
            ? LaneGroup.LaneRange(__BlobSlotsCount)
            : (0, __BlobSlotsCount);

        AsyncTick(Min, Max);
    }

    /// <summary>
    /// Evict expired blobs from a given slot range.
    /// 
    ///     - read scan:
    ///         - check if any node should be evicted
    /// 
    ///     - write scan:
    ///         - if nodes should be evicted, evict them!
    /// 
    /// A blob is evicted when:
    ///     - It has not been accessed within <c>__BlobEvictionTimeUSecs</c>
    ///     - No keys reference it
    ///     - No downstream consumers reference it
    /// </summary>
    public void AsyncTick(int SlotRangeMin, int SlotRangeMax)
    {
        var EvictionParams = new ExpireParams(__BlobEvictionTimeUSecs, 2);

        for (int SlotIndex = SlotRangeMin; SlotIndex < SlotRangeMax; ++SlotIndex)
        {
            var Slot = __BlobSlots[SlotIndex];
            var Stripe = __BlobStripes.FromSlot((ulong) SlotIndex);
            int StripeIndex = __BlobStripes.StripeIndex((ulong) SlotIndex);
            bool HasWork = false;

            // check for evictions
            using (Stripe.EnterRead())
            {
                for (var Node = Slot.Head; Node != null; Node = Node.Next)
                {
                    long KeyRefs = Volatile.Read(ref Node.KeyReferenceCount);
                    long DownRefs = Volatile.Read(ref Node.DownstreamReferenceCount);

                    if (Access.IsExpired(Node.Point, EvictionParams) && KeyRefs == 0 && DownRefs == 0)
                    {
                        HasWork = true;
                        break;
                    }
                }
            }

            if (!HasWork)
                continue;

            // if evictions were found, evict them!
            using (Stripe.EnterWrite())
            {
                for (var Node = Slot.Head; Node != null; )
                {
                    var Next = Node.Next;
                    long KeyRefs = Volatile.Read(ref Node.KeyReferenceCount);
                    long DownRefs = Volatile.Read(ref Node.DownstreamReferenceCount);

                    if (Access.IsExpired(Node.Point, EvictionParams) && KeyRefs == 0 && DownRefs == 0)
                    {
                        DllRemove(ref Slot.Head, ref Slot.Tail, Node);
                        Node.Reset();
                        Node.Next = __BlobFreeNodes[StripeIndex];
                        __BlobFreeNodes[StripeIndex] = Node;
                    }

                    Node = Next;
                }
            }
        }
    }

    /// <summary>
    /// Run eviction over all blob slots.
    /// </summary>
    public void AsyncTick()
    {
        AsyncTick(0, __BlobSlotsCount);
    }

    /// <summary>
    /// Register a given key under corresponding Root.
    /// </summary>
    private void RegisterKeyUnderRoot(ContentKey Key)
    {
        ulong RootHash = Hashing.HashStruct(Key.Root);
        ulong RootSlotIndex = RootHash % (ulong) __RootSlotsCount;
        var Slot = __RootSlots[RootSlotIndex];
        var Stripe = __RootStripes.FromSlot(RootSlotIndex);

        using (Stripe.EnterWrite())
        {
            for (var Node = Slot.Head; Node != null; Node = Node.Next)
            {
                if (Node.Root == Key.Root)
                {
                    Node.IDs.Add(Key.ID);
                    return;
                }
            }
        }
    }

    /// <summary>
    /// Decrement a blob's key reference count.
    /// </summary>
    private void DecrementBlobKeyRef(Hash128 Hash)
    {
        ulong SlotIndex = Hash.Hi % (ulong) __BlobSlotsCount;
        var Slot = __BlobSlots[SlotIndex];
        var Stripe = __BlobStripes.FromSlot(SlotIndex);

        using (Stripe.EnterRead())
        {
            for (var Node = Slot.Head; Node != null; Node = Node.Next)
            {
                if (Node.BlobHash == Hash)
                {
                    Interlocked.Decrement(ref Node.KeyReferenceCount);
                    break;
                }
            }
        }
    }

    private static void DllPushBack(ref BlobNode? Head, ref BlobNode? Tail, BlobNode Node)
    {
        Node.Prev = Tail;
        Node.Next = null;

        if (Tail != null)
            Tail.Next = Node;
        else
            Head = Node;

        Tail = Node;
    }

    private static void DllPushBack(ref KeyNode? Head, ref KeyNode? Tail, KeyNode Node)
    {
        Node.Prev = Tail;
        Node.Next = null;

        if (Tail != null)
            Tail.Next = Node;
        else
            Head = Node;

        Tail = Node;
    }
    private static void DllPushBack(ref RootNode? Head, ref RootNode? Tail, RootNode Node)
    {
        Node.Prev = Tail;
        Node.Next = null;

        if (Tail != null)
            Tail.Next = Node;
        else
            Head = Node;

        Tail = Node;
    }

    private static void DllRemove(ref BlobNode? Head, ref BlobNode? Tail, BlobNode Node)
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

    private static void DllRemove(ref KeyNode? Head, ref KeyNode? Tail, KeyNode Node)
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
    private static void DllRemove(ref RootNode? Head, ref RootNode? Tail, RootNode Node)
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
        __BlobStripes.Dispose();
        __KeyStripes.Dispose();
        __RootStripes.Dispose();
    }
}