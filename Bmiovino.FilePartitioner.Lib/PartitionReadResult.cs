namespace FilePartitioner;

public class PartitionReadResult<T> : PartitionActionStatus where T : class, new()
{
    public PartitionReadResult(ParitionStatusEnum paritionStatusEnum, string message) : base(paritionStatusEnum, message) { }

    public PartitionRecord PartitionRecord { get; set; }

    public List<T>? Data { get; set; }
}