namespace ExternalSort;

public record Scalar<T>(T Value) : IComparable<Scalar<T>>
{
    
    private readonly Comparer<T> _comparer = Comparer<T>.Default;
    
    public Scalar(): this(default(T))
    {
    }

    public int CompareTo(Scalar<T> other)
    {
        return _comparer.Compare(Value, other.Value);
    }

    
}