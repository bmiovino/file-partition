using FilePartitioner;
using System.Diagnostics;

namespace FilePartitionerTest;

public class ReadPartitionsTest
{
    [Fact]
    public void ReadPartitionDataTest()
    {
        #region #------------------------------- Assemble --------------------------------------#

        var directory = @"C:\Temp_File_Partitioner_ReadTests\";

        if (!Directory.Exists(directory))
            Directory.CreateDirectory(directory);

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

        var res = filePartitioner.WritePartitions(data, 9);

        Assert.Equal(PartitionActionStatus.ParitionStatusEnum.Success, res.Status);

        #endregion

        #region #------------------------------- Act --------------------------------------#
        FilePartitioner<TestData> filePartitionerReader = new FilePartitioner.FilePartitioner<TestData>(new CsvFileReaderWriter<TestData>(), directory, filebase, "csv");

        var readRes = new List<PartitionReadResult<TestData>?>();

        for (int i = 0; i < 12; i++)
        {
            var readItem = filePartitionerReader.ReadPartition(i);
            readRes.Add(readItem);
        }

        #endregion

        #region #------------------------------- Assert --------------------------------------#

        for (int i = 0; i < 12; i++)
        {
            Assert.NotNull(readRes[i]);
            Assert.Equal(PartitionActionStatus.ParitionStatusEnum.Success, readRes[i]!.Status);
            if (i < 11)
                Assert.Equal(9, readRes[i]!.Data!.Count());
            else
                Assert.Single(readRes[i]!.Data!);
        }

        #endregion
    }
}
