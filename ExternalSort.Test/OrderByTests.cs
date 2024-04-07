namespace ExternalSort.Test;

public class OrderByTests
{

    

    
    [Fact]
    public async Task HappyPath_1TempFile()
    {
        var source = await RowGenerator.GenerateUsers(10).ToListAsync();

        var actual = await source.ToAsyncList()
            .OrderByExternal(u => u.Email)
            .OptimiseFor(calculateBytesInRam: u => u.CalculateSize())
            .ToListAsync();

        actual.Should().HaveCount(source.Count);

        actual.Should().Equal(source.OrderBy(u => u.Email).ToList());
    }
    
    
    
    
    [Fact]
    public async Task HappyPath_2TempFiles()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source.ToAsyncList()
            .OrderByExternal(u => u.Email)
            .OptimiseFor(calculateBytesInRam: u => u.CalculateSize() * 100 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u.Email).ToList();
        
        actual.Should().Equal(expected);
    }

    
    
    [Fact]
    public async Task HappyPath_2TempFilesDescending()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source.ToAsyncList()
            .OrderByDescendingExternal(u => u.Email)
            .OptimiseFor(calculateBytesInRam: u => u.CalculateSize() * 100 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderByDescending(u => u.Email).ToList();
        
        actual.Should().Equal(expected);
    }

        
    
    [Fact]
    public async Task HappyPath_2TempFilesMultiColumn()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source.ToAsyncList()
            .OrderByExternal(u => u.Firstname)
            .ThenBy(u => u.SomethingUnique)
            .OptimiseFor(calculateBytesInRam: u => u.CalculateSize() * 100 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u.Firstname).ThenBy(u => u.SomethingUnique).ToList();
        
        actual.Should().Equal(expected);
    }


    [Fact]
    public async Task HappyPath_2TempFilesMultiColumnDescending()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source.ToAsyncList()
            .OrderByExternal(u => u.Firstname)
            .ThenByDescending(u => u.SomethingUnique)
            .OptimiseFor(calculateBytesInRam: u => u.CalculateSize() * 100 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u.Firstname).ThenByDescending(u => u.SomethingUnique).ToList();
        
        actual.Should().Equal(expected);
    }


    
    [Fact]
    public async Task HappyPath_20TempFiles()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source.ToAsyncList()
            .OrderByExternal(u => u.Email)
            .OptimiseFor(calculateBytesInRam: u => u.CalculateSize() * 1_000 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);

        actual.Should().Equal(source.OrderBy(u => u.Email).ToList());
    }

    
    
    [Fact]
    public async Task HappyPath_110TempFiles()
    {
        var sourceCount = 110_000;
        var source = RowGenerator.GenerateUsers(sourceCount);

        var actual = source
            .OrderByExternal(u => u.Email)
            .OptimiseFor(calculateBytesInRam: u => 1_000, mbLimit: 1, openFilesLimit: 10);

        var actualCount = 0;
        await foreach (var row in actual)
            actualCount++;

        actualCount.Should().Be(sourceCount);
    }

    
    
    [Fact(Skip = "Takes 5 minutes")]
    public async Task HappyPath_10MillionSpeedTest()
    {
        var sourceCount = 10_000_000;
        var source = RowGenerator.GenerateUsers(sourceCount);

        var actual = source
            .OrderByExternal(u => u.Email)
            .OptimiseFor(mbLimit: 50);

        var actualCount = 0;
        await foreach (var row in actual)
            actualCount++;

        actualCount.Should().Be(sourceCount);
    }

    
    [Fact]
    public async Task Error_InvalidCalculateBytesInRam()
    {
        var sourceCount = 110_000;
        var source = RowGenerator.GenerateUsers(sourceCount);

        var actual = source
            .OrderByExternal(u => u.Email)
            .OptimiseFor(calculateBytesInRam: u => 0);

        var actualCount = 0;
        
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var row in actual)
                actualCount++;
        });
       

    }
    
    
    [Fact]
    public async Task Error_InvalidMbLimit()
    {
        var sourceCount = 110_000;
        var source = RowGenerator.GenerateUsers(sourceCount);
        
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            var actual = source
                .OrderByExternal(u => u.Email)
                .OptimiseFor(mbLimit: 0);
        });
    }
    
    [Fact]
    public async Task Error_InvalidOpenFilesLimit()
    {
        var sourceCount = 110_000;
        var source = RowGenerator.GenerateUsers(sourceCount);
        
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            var actual = source
                .OrderByExternal(u => u.Email)
                .OptimiseFor(openFilesLimit: 1);
        });
    }
}


