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

        var actual = await source.ToAsyncList().OrderByExternal(u => u.Email, u => u.CalculateSize() * 100 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u.Email).ToList();
        
        actual.Should().Equal(expected);
    }

    
    [Fact]
    public async Task HappyPath_20TempFiles()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source.ToAsyncList().OrderByExternal(u => u.Email, u => u.CalculateSize() * 1_000 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);

        actual.Should().Equal(source.OrderBy(u => u.Email).ToList());
    }

    
    
    [Fact]
    public async Task HappyPath_HugeNumberOfTempFiles()
    {
        var sourceCount = 100_000_000;
        var source = RowGenerator.GenerateUsers(sourceCount);

        var actual = source.OrderByExternal(u => u.Email, u => u.CalculateSize());

        var actualCount = 0;
        await foreach (var row in actual)
            actualCount++;

        actualCount.Should().Be(sourceCount);
    }

}