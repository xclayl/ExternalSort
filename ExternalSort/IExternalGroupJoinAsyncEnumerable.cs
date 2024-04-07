namespace ExternalSort;

public interface IExternalGroupJoinAsyncEnumerable<out TOuter, out TInner, out TResult> : IAsyncEnumerable<TResult>
{
    IExternalGroupJoinAsyncEnumerable<TOuter, TInner, TResult> OptimiseFor(Func<TOuter, long>? calculateOuterBytesInRam = null, Func<TInner, long>? calculateInnerBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null);
}
