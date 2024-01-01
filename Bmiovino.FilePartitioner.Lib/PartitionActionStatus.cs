namespace FilePartitioner;

public class PartitionActionStatus
{
    public PartitionActionStatus() { }

    public PartitionActionStatus(ParitionStatusEnum paritionStatusEnum, string message)
    {
        Status = paritionStatusEnum;
        Message = message;
    }

    public ParitionStatusEnum Status { get; set; } = ParitionStatusEnum.None;
    public string Message = "";

    public enum ParitionStatusEnum
    {
        None = 0,
        Success = 1,
        Error = 2
    }
}
