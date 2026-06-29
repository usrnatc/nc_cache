using System;
using System.Threading;
using System.Threading.Tasks;

namespace NcCache;

/// <summary>
/// A <c>CacheHandle</c> is a handle to cached file data.
/// Wraps a ReadOnlyMemory alongside an AccessScope.
/// The AccessScope keeps the underlying data blob alive
/// until <c>Dispose()</c> is called, the data blob's reference count 
/// is decremented and may be evicted.
/// </summary>
public readonly struct CacheHandle : IDisposable
{
    private readonly Access __Acc;
    public readonly ReadOnlyMemory<byte> Data;

    internal CacheHandle(Access Acc, ReadOnlyMemory<byte> Data)
    {
        this.__Acc = Acc;
        this.Data = Data;
    }

    /// <summary>
    /// Release our Access Scope so the cached data may be evicted.
    /// </summary>
    public void Dispose()
    {
        __Acc?.Dispose();
    }
}

/// <summary>
/// Cache engine orchestrating all caching subsystems:
///     - a pool of background threads processing cache work
///     - a <c>ContentCache</c> for content-addressed storage
///     - a <c>FileStreamCache</c> for file change detection and fast-path lookups
///     - an <c>ObjectCacheManager</c> for cached data eviction and request scheduling
/// 
/// Example usage:
/// <code>
/// var engine = new NcCacheEngine();
/// 
/// engine.Start();
/// 
/// using var handle = await engine.ReadFileMultiThreaded(
///     "path/to/file",
///     TimeSpan.FromSeconds(5)
/// );
/// 
/// // ~ use data
/// // handle.Dispose() if not needed anymore
/// 
/// engine.Dispose();
/// </code>
/// </summary>
public sealed class NcCacheEngine : IDisposable
{
    private readonly ContentCache __CCache;
    private readonly FileStreamCache __FCache;
    private readonly ObjectCacheManager __CacheManager;
    private readonly LaneGroup __Lanes;
    private readonly Thread[] __AsyncThreads;
    private readonly int __LaneCount;
    private bool __IsRunning;
    private bool __IsDisposed;

    public NcCacheEngine(int LanesCount = 0)
    {
        // NOTE(nathan): default to one <c>Stripe</c> per processor
        __LaneCount = LanesCount > 0 ? LanesCount : Math.Max(1, Environment.ProcessorCount);
        __CCache = new ContentCache();
        __FCache = new FileStreamCache(__CCache);
        __CacheManager = new ObjectCacheManager();
        __CacheManager.Register(__FCache.Cache());
        __Lanes = new LaneGroup(__LaneCount);
        __AsyncThreads = new Thread[__LaneCount];
    }

    /// <summary>
    /// Kick off background threads, each will:
    ///     - process cache management (eviction + handle requests)
    ///     - check tracked files for modifications since last check
    ///     - evict expired data blobs
    ///     - advance global async tick
    ///     - sleep for 20ms if no data or no repeats were requested
    /// 
    /// All threads are coordinated by wait barriers for work that has requested
    /// multi-threaded processing.
    /// </summary>
    public void Start()
    {
        if (__IsRunning)
            return;

        __IsRunning = true;

        for (int Index = 0; Index < __LaneCount; ++Index)
        {
            int LIndex = Index;

            __AsyncThreads[Index] = new Thread(() =>
            {
                __Lanes.SetThreadContext(LIndex);

                try
                {
                    while (Volatile.Read(ref __IsRunning))
                    {
                        // cache eviction and request creation
                        __CacheManager.Tick(__Lanes);

                        // check for file modifications
                        __FCache.AsyncTick(__Lanes);

                        // evict expired data blobs
                        __CCache.AsyncTick(__Lanes);

                        if (LIndex == 0)
                            UpdateTick.Advance();

                        __Lanes.Sync();

                        if (LIndex == 0)
                            if (!AsyncLoopSignal.ConsumeRepeatRequested())
                                Thread.Sleep(20);                               // NOTE: nothing pending => take a nap

                        __Lanes.Sync();
                    }
                }
                catch
                {
                    // NOTE: Any single lane crashing causes all Lanes to be stopped
                    Volatile.Write(ref __IsRunning, false);
                }
            })
            {
                Name = $"[LANE_{LIndex}]",
                IsBackground = true
            };

            __AsyncThreads[Index].Start();
        }
    }

    /// <summary>
    /// Halt all running threads and wait for all to stop
    /// </summary>
    public void Stop()
    {
        if (!__IsRunning)
            return;

        Volatile.Write(ref __IsRunning, false);
        AsyncLoopSignal.RequestRepeat();

        for (int Index = 0; Index < __AsyncThreads.Length; ++Index)
        {
            var T = __AsyncThreads[Index];

            if (T != null && T.IsAlive)
                T.Join();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="Path"></param>
    /// <param name="Timeout"></param>
    /// <param name="Flags"></param>
    /// <returns></returns>
    /// <exception cref="TimeoutException"></exception>
    public ValueTask<CacheHandle> ReadFileMultiThreaded(
        string Path,
        TimeSpan Timeout,
        CacheFlags Flags = CacheFlags.None
    )
    {
        Access Acc = Access.Open();
        long NowUSecs = TimeUtil.NowUSecs();
        var Data = __FCache.ReadFile(Acc, Path, NowUSecs, Flags);

        if (!Data.IsEmpty)
            return new ValueTask<CacheHandle>(new CacheHandle(Acc, Data));

        Acc.Dispose();

        return new ValueTask<CacheHandle>(Task.Run(() =>
        {
            long EndTimeUSecs = TimeUtil.NowUSecs() + TimeUtil.USecsFromSecs(Timeout.TotalSeconds);
            Access AsyncAcc = Access.Open();
            var AsyncData = __FCache.ReadFile(AsyncAcc, Path, EndTimeUSecs, Flags);

            if (AsyncData.IsEmpty)
            {
                AsyncAcc.Dispose();

                throw new TimeoutException($"cache read timed out for {Path}");
            }

            return new CacheHandle(AsyncAcc, AsyncData);
        }));
    }

    /// <summary>
    /// Read a file using current thread's cache.
    /// Blocks until data is available or Timeout is reached and/or exceeded.
    /// Throws TimeoutException if the data is not available in time.
    /// </summary>
    /// <param name="Path">Path of file to be read</param>
    /// <param name="Timeout">Max time of retrieval</param>
    /// <param name="Flags">Determines how file is retrieved</param>
    /// <returns>Handle to cached data of file</returns>
    /// <exception cref="TimeoutException"></exception>
    public CacheHandle ReadFileSingleThreaded(
        string Path,
        long Timeout,
        CacheFlags Flags = CacheFlags.None
    )
    {
        long EndTimeUSecs = TimeUtil.NowUSecs() + Timeout;
        Access Acc = Access.Open();
        var Data = __FCache.ReadFile(Acc, Path, EndTimeUSecs, Flags);

        if (Data.IsEmpty)
        {
            Acc.Dispose();

            throw new TimeoutException($"cache read timed out for {Path}");
        }

        return new CacheHandle(Acc, Data);
    }

    /// <summary>
    /// Stop background threads and dispose of all subsystems.
    /// This order should be in reverse dependancy order.
    /// </summary>
    public void Dispose()
    {
        if (__IsDisposed)
            return;

        __IsDisposed = true;
        Stop();
        __CacheManager.Dispose();
        __FCache.Dispose();
        __CCache.Dispose();
        __Lanes.Dispose();
    }
}
