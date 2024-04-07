namespace ExternalSort;

public interface IExternalOrderByAsyncEnumerable<out T> : IAsyncEnumerable<T> where T : new()
{
    IExternalOrderByAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null);
    IExternalOrderByAsyncEnumerable<T> ThenBy<TK>(Func<T, TK> keySelector);
    IExternalOrderByAsyncEnumerable<T> ThenByDescending<TK>(Func<T, TK> keySelector);
    
}
