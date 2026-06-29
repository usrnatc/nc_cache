[![NuGet Version](https://img.shields.io/nuget/v/NcCache.svg?style=for-the-badge&logo=nuget)](https://www.nuget.org/packages/NcCache/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/NcCache.svg?style=for-the-badge&logo=nuget)](https://www.nuget.org/packages/NcCache/)

# NcCache
High-performance, multi-threaded, content-addressable caching engine for C#.

## About
**NcCache** is a C# library for caching file data and objects using content-addressable storage. It is designed for high concurrency and low overhead, utilizing striped reader-writer locks, background worker threads (Lanes), and reference-counted eviction.

The engine consists of three main layers:
* **`ContentCache`**: A content-addressable data store mapping 128-bit hashes to byte arrays.
* **`FileStreamCache`**: A file caching layer that detects file modifications and provides fast-path lookups.
* **`ObjectCache<T>`**: A generic object cache with generation tracking and asynchronous request processing.

All state is managed by the `NcCacheEngine`, which orchestrates background threads to process cache requests, evict expired blobs, and detect file changes without blocking the main application.

## Installation

### Manual
Copy the `.cs` files into your C# project. 

> [!WARNING]
> The library requires **.NET 8 or later** due to its reliance on `System.IO.Hashing` (`XxHash3`, `XxHash128`) and modern `Span<T>` / `Memory<T>` primitives.

### .NET CLI
```bash
dotnet add package NcCache
```

## Usage

### Initialisation
Create and start the `NcCacheEngine`. By default, it allocates one background thread (Lane) per processor core to handle cache eviction, file monitoring, and parallel reading.

```csharp
using NcCache;

// Initialize the engine
var engine = new NcCacheEngine();

// Start background worker threads
engine.Start();
```

### Reading files
Files can be read asynchronously or synchronously. The engine handles deduplication, file modification tracking, and parallel reading across multiple lanes automatically.

```csharp
// Asynchronous multi-threaded read with a 5-second timeout
using (var handle = await engine.ReadFileMultiThreaded("data.bin", TimeSpan.FromSeconds(5)))
{
    // Access the cached data
    ReadOnlyMemory<byte> data = handle.Data;
    
    // Process data...
} // handle.Dispose() releases the access scope, allowing the data to be evicted
```

```csharp
// Synchronous read blocking the current thread
long timeoutUSecs = TimeUtil.USecsFromSecs(5.0);
using (var handle = engine.ReadFileSingleThreaded("data.bin", timeoutUSecs))
{
    ReadOnlyMemory<byte> data = handle.Data;
}
```

> [!TIP]
> You can pass `CacheFlags.HighPriority` to bump a read request to the front of the processing queue, or `CacheFlags.WaitForFresh` to ensure you don't get a stale cache hit while a file modification is actively being processed.

### Shutdown
Always dispose the engine to cleanly halt background threads and release resources.

```csharp
engine.Stop();
engine.Dispose();
```

## Architecture

### Content-Addressable Storage
`ContentCache` deduplicates data by hashing content using `XxHash128`. If two files (or two versions of a file) have identical content, they share the same underlying byte array in memory. Older hashes are kept in a rolling history ring buffer, allowing the cache to rewind to previous versions if the latest version is not yet available.

### Striped Locking
To minimize thread contention, hash tables (`BlobSlot`, `KeySlot`, `RootSlot`, `FSSlot`) are protected by a `StripeArray`. A custom `Stripe` struct implements a highly optimized reader-writer lock using `Interlocked` operations and `SpinWait`, avoiding heavy OS-level mutexes on the hot path.

### LaneGroups
Background threads are grouped into `LaneGroup`s. When a large file needs to be read, the work is distributed across all available lanes, coordinated by a `Barrier`. Lane 0 handles file handles and metadata, while all lanes read their respective chunks in parallel using `RandomAccess.Read`.

### Eviction
Data blobs are evicted based on a combination of time and reference counting. An `AccessPoint` tracks the last time a blob was touched and its active reference count. Background threads periodically scan the cache and evict blobs that have expired and have no active downstream consumers.

## Examples

### Complete round-trip

```csharp
using System;
using System.Threading.Tasks;
using NcCache;

class Program
{
    static async Task Main()
    {
        using var engine = new NcCacheEngine();
        engine.Start();

        try
        {
            // Request a file, waiting up to 2 seconds for the background lanes to cache it
            using var handle = await engine.ReadFileMultiThreaded(
                "config.json", 
                TimeSpan.FromSeconds(2)
            );
            
            Console.WriteLine($"Successfully read {handle.Data.Length} bytes.");
        }
        catch (TimeoutException)
        {
            Console.WriteLine("Cache read timed out.");
        }
        
        engine.Stop();
    }
}
```

## Limitations

* **Memory**: File reads allocate arrays from `ArrayPool<byte>.Shared`. Extremely large files might cause pool exhaustion or Large Object Heap (LOH) fragmentation if not sized carefully.
* **64-bit Optimized**: The hashing and internal structures heavily utilize `ulong` and 64-bit arithmetic, which may perform sub-optimally on 32-bit runtimes.
