using System.Runtime.CompilerServices;
using System.Threading;

namespace NcCache;

public sealed class CancelToken
{
    private int __Value;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsCancelled()
    {
        return Volatile.Read(ref __Value) != 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Cancel()
    {
        Volatile.Write(ref __Value, 1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset()
    {
        Volatile.Write(ref __Value, 0);
    }
}