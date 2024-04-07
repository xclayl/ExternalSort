namespace ExternalSort.Test;

public class ExceptByTests
{
        
    [Fact]
    public void ExceptByBehaviour()
    {
        List<int> mainList = [1, 3];
        List<int> excludeList = [1, 2];

        var actual = mainList
            .ExceptBy(excludeList, m => m)
            .ToList();

        actual.Should().HaveCount(1);
        actual.Should().BeEquivalentTo([3]);
    }

         
    [Fact]
    public void ExceptByBehaviourWithDuplicates()
    {
        List<(int, int)> mainList = [(1, 100), (3, 300), (3, 301)];
        List<int> excludeList = [1, 2];

        var actual = mainList
            .ExceptBy(excludeList, m => m.Item1)
            .ToList();

        actual.Should().HaveCount(1);
        actual.Should().BeEquivalentTo([(3, 300)]);
    }

        
    [Fact]
    public async Task ExceptByExternalBehaviour()
    {
        List<int> mainList = [1, 3];
        List<int> excludeList = [1, 2];

        var actual = await mainList.ToAsyncList()
            .ExceptByExternal(excludeList.ToAsyncList(), m => m)
            .ToListAsync();

        actual.Should().HaveCount(1);
        actual.Should().BeEquivalentTo([3]);
    }

    
         
    [Fact]
    public async Task ExceptByExternalBehaviourWithDuplicates()
    {
        List<(int, int)> mainList = [(1, 100), (3, 300), (3, 301)];
        List<int> excludeList = [1, 2];
        
        var actual = await mainList.ToAsyncList()
            .ExceptByExternal(excludeList.ToAsyncList(), m => m.Item1)
            .ToListAsync();

        actual.Should().HaveCount(1);
        actual.Should().BeEquivalentTo([(3, 300)]);
    }

}