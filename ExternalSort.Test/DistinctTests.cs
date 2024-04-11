namespace ExternalSort.Test;

public class DistinctTests
{
    
    [Fact]
    public async Task HappyPath_1TempFile()
    {
        var source = await RowGenerator.GenerateUsers(100).Select(u => u.Firstname).ToListAsync();

        var actual = await source.ToAsyncList()
            .Select(s => new Scalar<string>(s))
            .DistinctExternal()
            .OptimiseFor(calculateBytesInRam: u => u.Value.Length * 2)
            .Select(s => s.Value)
            .ToListAsync();

        actual.Should().HaveCount(source.Distinct().Count());

        actual.Should().Equal(source.Distinct().OrderBy(u => u).ToList());
    }

    
    [Fact]
    public async Task HappyPath_1TempFileInts()
    {
        var source = await RowGenerator.GenerateUsers(1_000).Select(u => u.Firstname.Length).ToListAsync();

        var actual = await source.ToAsyncList()
            .DistinctExternal()
            .OptimiseFor(calculateBytesInRam: u => 4)
            .ToListAsync();

        actual.Should().HaveCount(source.Distinct().Count());

        actual.Should().Equal(source.Distinct().OrderBy(u => u).ToList());
    }
}