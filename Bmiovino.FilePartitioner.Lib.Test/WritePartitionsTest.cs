using FilePartitioner;

namespace FilePartitionerTest;

public class WritePartitionsTest
{
    [Fact]
    public void WritePartitionDataTest()
    {
        var directory = @"C:\Temp_File_Partitioner_Tests\";

        if (!Directory.Exists(directory))
            Directory.CreateDirectory(directory);

        //clean out the files
        var files = Directory.GetFiles(directory, "*.csv");
        foreach (var file in files)
        {
            File.Delete(file);
        }

        var filebase = "file";
        
        FilePartitioner<TestData> filePartitioner = new FilePartitioner.FilePartitioner<TestData>(new CsvFileReaderWriter<TestData>(), directory, filebase, "csv");
        
        var data = new List<TestData>();

        for(int i = 0; i < 100; i++)
            data.Add(new TestData(i));

        var res = filePartitioner.WritePartitions(data, 9);

        Assert.Equal(PartitionActionStatus.ParitionStatusEnum.Success, res.Status);
        Assert.Equal(0, filePartitioner.MinIndex);
        Assert.Equal(99, filePartitioner.MaxIndex);
        Assert.Equal(12, filePartitioner.NumberOfPartitions);
    }

    [Fact]
    public void WriteSinglePartitionDataTest()
    {
        var directory = @"C:\Temp_File_Partitioner_Single_Tests\";

        if (!Directory.Exists(directory))
            Directory.CreateDirectory(directory);

        //clean out the files
        var files = Directory.GetFiles(directory, "*.csv");
        foreach (var file in files)
        {
            File.Delete(file);
        }

        var filebase = "file";

        FilePartitioner<TestData> filePartitioner = new FilePartitioner.FilePartitioner<TestData>(new CsvFileReaderWriter<TestData>(), directory, filebase, "csv");

        var data = new List<TestData>();

        for (int i = 0; i < 100; i++)
            data.Add(new TestData(i));

        var res = filePartitioner.WriteSinglePartition(data, indexOffset: 25);

        Assert.Equal(PartitionActionStatus.ParitionStatusEnum.Success, res.Status);
        Assert.Equal(25, filePartitioner.MinIndex);
        Assert.Equal(124, filePartitioner.MaxIndex);
        Assert.Equal(1, filePartitioner.NumberOfPartitions);

        var res2 = filePartitioner.WriteSinglePartition(data, indexOffset: 125);

        Assert.Equal(PartitionActionStatus.ParitionStatusEnum.Success, res2.Status);
        Assert.Equal(125, filePartitioner.MinIndex);
        Assert.Equal(224, filePartitioner.MaxIndex);
        Assert.Equal(1, filePartitioner.NumberOfPartitions);

    }
}