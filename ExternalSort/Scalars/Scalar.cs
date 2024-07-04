namespace ExternalSort.Scalars;

/// <summary>
/// allows sorting a one column list using the default C# sort order for that type.
/// For strings, OrdinalString, OrdinalIgnoreCaseString, InvariantCultureString, InvariantCultureIgnoreCaseString
/// are better options, which specify a sort order as well.
/// </summary>
public record Scalar<T>(T Value) : IComparable<Scalar<T>>
{
    
    private readonly Comparer<T> _comparer = Comparer<T>.Default;
    
    public Scalar(): this((T)default)
    {
    }

    public int CompareTo(Scalar<T> other)
    {
        return _comparer.Compare(Value, other.Value);
    }

    
}