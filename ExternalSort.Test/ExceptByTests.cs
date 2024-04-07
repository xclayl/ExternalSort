namespace ExternalSort.Test;

public class ExceptByTests
{
        
    [Fact]
    public void ExceptBehaviour()
    {
        List<int> mainList = [1, 3];
        List<int> excludeList = [1, 2];

        var actual = mainList
            .ExceptBy(excludeList, m => m)
            .ToList();

        actual.Should().HaveCount(1);
        actual.Should().BeEquivalentTo([3]);
    }

}