using ExternalSort.Scalars;

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
    public async Task HappyPath_SpecifySortOrder()
    {
        
        List<string> sourceA = ["a", "A"];
        var source = sourceA.Select(s => new OrdinalString(s)).ToList();

        var actual = await source.ToAsyncList()
            .OrderByExternal()
            .OptimiseFor(calculateBytesInRam: u => u.Value.Length)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);

        actual.Should().Equal(source.OrderBy(u => u).ToList());
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
    public async Task HappyPath_2TempFilesWithExplicitScalar()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source
            .Select(u => new Scalar<string> { Value = u.Email })
            .ToAsyncList()
            .OrderByExternal(u => u)
            .OptimiseFor(calculateBytesInRam: u => 1_000 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u.Email).Select(u => u.Email).ToList();
        
        actual.Select(u => u.Value).Should().Equal(expected);
    }

       
    [Fact]
    public async Task HappyPath_2TempFilesWithTempDir()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        var actual = await source
            .ToAsyncList()
            .OrderByExternal(u => u.SomethingUnique)
            .OptimiseFor(calculateBytesInRam: u => 1_000 /* pretend these use more RAM */)
            .UseTempDir(Path.Combine(Path.GetTempPath(), $"abc_{Guid.NewGuid()}"))
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u.SomethingUnique).ToList();
        
        actual.Select(u => u.SomethingUnique).Should().Equal(expected.Select(u => u.SomethingUnique));
    }


        
    [Fact]
    public async Task HappyPath_2TempFilesWithInt()
    {
        var source = await RowGenerator.GenerateUsers(10_000).Select((e, i) => i).ToListAsync();

        var actual = await source
            .ToAsyncList()
            .OrderByExternal(u => u)
            .OptimiseFor(calculateBytesInRam: u => 1_000 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u).Select(u => u).ToList();
        
        actual.Select(u => u).Should().Equal(expected);
    }

        
    [Fact]
    public async Task HappyPath_2TempFilesWithLong()
    {
        var source = await RowGenerator.GenerateUsers(10_000).Select((u, i) => (long)i).ToListAsync();

        var actual = await source
            .ToAsyncList()
            .OrderByExternal(u => u)
            .OptimiseFor(calculateBytesInRam: u => 1_000 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u).Select(u => u).ToList();
        
        actual.Select(u => u).Should().Equal(expected);
    }

        
    [Fact]
    public async Task HappyPath_2TempFilesWithGuid()
    {
        var source = await RowGenerator.GenerateUsers(10_000).Select(u => u.UserGuid).ToListAsync();

        var actual = await source
            .ToAsyncList()
            .OrderByExternal(u => u)
            .OptimiseFor(calculateBytesInRam: u => 1_000 /* pretend these use more RAM */)
            .ToListAsync();

        actual.Should().HaveCount(source.Count);
        
        var expected = source.OrderBy(u => u).Select(u => u).ToList();
        
        actual.Select(u => u).Should().Equal(expected);
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
    
        
    [Fact]
    public async Task CancellationToken()
    {
        var source = await RowGenerator.GenerateUsers(10_000).ToListAsync();

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            var actual = await source.ToAsyncList()
                .OrderByExternal(u => u.Email, cts.Token)
                .OptimiseFor(u => 100_000)
                .ToListAsync();
        });
    }
}


