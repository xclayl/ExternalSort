namespace ExternalSort.Scalars;


/// <summary>
/// Specifies how to sort the strings.  
/// </summary>
public record OrdinalString(string? Value) : IComparable<OrdinalString>
{
    private static readonly StringComparer Comparer = StringComparer.Ordinal;
    
    public OrdinalString(): this((string?)default)
    {
    }

    public int CompareTo(OrdinalString other)
    {
        return Comparer.Compare(Value, other.Value);
    }
}

/// <summary>
/// Specifies how to sort the strings
/// </summary>
public record OrdinalIgnoreCaseString(string? Value) : IComparable<OrdinalIgnoreCaseString>
{
    private static readonly StringComparer Comparer = StringComparer.OrdinalIgnoreCase;
    
    public OrdinalIgnoreCaseString(): this((string?)default)
    {
    }

    public int CompareTo(OrdinalIgnoreCaseString other)
    {
        return Comparer.Compare(Value, other.Value);
    }
}

/// <summary>
/// Specifies how to sort the strings
/// </summary>
public record InvariantCultureString(string? Value) : IComparable<InvariantCultureString>
{
    private static readonly StringComparer Comparer = StringComparer.InvariantCulture;
    
    public InvariantCultureString(): this((string?)default)
    {
    }

    public int CompareTo(InvariantCultureString other)
    {
        return Comparer.Compare(Value, other.Value);
    }
}

/// <summary>
/// Specifies how to sort the strings
/// </summary>
public record InvariantCultureIgnoreCaseString(string? Value) : IComparable<InvariantCultureIgnoreCaseString>
{
    private static readonly StringComparer Comparer = StringComparer.InvariantCultureIgnoreCase;
    
    public InvariantCultureIgnoreCaseString(): this((string?)default)
    {
    }

    public int CompareTo(InvariantCultureIgnoreCaseString other)
    {
        return Comparer.Compare(Value, other.Value);
    }
}