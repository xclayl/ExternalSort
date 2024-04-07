namespace ExternalSort;

public interface IExternalExceptByAsyncEnumerable<out TSource> : IAsyncEnumerable<TSource>
{
    IExternalExceptByAsyncEnumerable<TSource> OptimiseFor(Func<TSource, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null);
}