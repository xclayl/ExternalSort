namespace ExternalSort.ExceptBy;

internal class ExternalExceptByAsyncEnumerable<TSource, TKey> : IExternalExceptByAsyncEnumerable<TSource> where TSource : new() where TKey : new()
{
    public ExternalExceptByAsyncEnumerable(IAsyncEnumerable<TSource> first, IAsyncEnumerable<TKey> second, Func<TSource,TKey> keySelector)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerator<TSource> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
    {
        throw new NotImplementedException();
    }

    public IExternalExceptByAsyncEnumerable<TSource> OptimiseFor(Func<TSource, long>? calculateBytesInRam = null, int? mbLimit = null,
        int? openFilesLimit = null)
    {
        throw new NotImplementedException();
    }
}