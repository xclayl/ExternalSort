namespace ExternalSort;

public interface IExternalOrderByAsyncEnumerable<out T> : IAsyncEnumerable<T> where T : new()
{
    IExternalOrderByAsyncEnumerable<T> OptimiseFor(Func<T, long>? calculateBytesInRam = null, int? mbLimit = null, int? openFilesLimit = null);
    IExternalOrderByAsyncEnumerable<T> UseTempDir(string dir);
    IExternalOrderByAsyncEnumerable<T> ThenBy<TK>(Func<T, TK> keySelector) where TK : IComparable<TK>;
    IExternalOrderByAsyncEnumerable<T> ThenByDescending<TK>(Func<T, TK> keySelector) where TK : IComparable<TK>;
    
    
}
