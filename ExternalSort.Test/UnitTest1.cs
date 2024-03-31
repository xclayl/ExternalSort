namespace ExternalSort.Test;

public class UnitTest1
{

    

    
    [Fact]
    public async Task HappyPath_1TempFile()
    {
        var source = await RowGenerator.GenerateUsers(10).ToListAsync();

        var actual = await source.ToAsyncList().OrderByExternal(u => u.Email, u => u.CalculateSize())
            .ToListAsync();

        actual.Should().HaveCount(source.Count);

        actual.Should().Equal(source.OrderBy(u => u.Email).ToList());
    }
    
    
    
    
    [Fact]
    public async Task HappyPath_2TempFiles()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source.ToAsyncList().OrderByExternal(u => u.SomethingUnique, u => u.CalculateSize() * 100 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u.SomethingUnique).ToList();
        
        actual.Should().Equal(expected);
    }

    
    [Fact]
    public async Task HappyPath_20TempFiles()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source.ToAsyncList().OrderByExternal(u => u.SomethingUnique, u => u.CalculateSize() * 1_000 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);

        actual.Should().Equal(source.OrderBy(u => u.SomethingUnique).ToList());
    }

    
}