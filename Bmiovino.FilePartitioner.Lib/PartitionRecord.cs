namespace FilePartitioner;

public record PartitionRecord
{
    public int Number { get; set; } = 0;
    public int MinIndex { get; set; } = 0;
    public int MaxIndex { get; set; } = 0;
}
