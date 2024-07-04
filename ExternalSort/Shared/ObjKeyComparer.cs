using ExternalSort.OrderBy;

namespace ExternalSort.Shared;

internal class ObjKeyComparer<T, TK> : IComparer<T> where TK : IComparable<TK>
{
    private readonly OrderByPair<T, TK> _orderByPair;

    public ObjKeyComparer(OrderByPair<T, TK> orderByPair)
    {
        _orderByPair = orderByPair;
    }
    
    public int Compare(T? x, T? y)
    {
        var xKey = _orderByPair.KeySelector(x);
        var yKey = _orderByPair.KeySelector(y);
        return (_orderByPair.OrderBy == OrderBy.OrderBy.Asc ? 1 : -1) * CompareUtil.Compare(xKey, yKey);
    }
}