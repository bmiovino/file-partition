
using FilePartitioner;
using System.Diagnostics;

namespace FilePartitionerTest
{
    public class WritePartitionsTest
    {
        [Fact]
        public void WritePartitionDataTest()
        {
            var directory = @"C:\Temp\Test\FP1\";

            //clean out the files
            var files = Directory.GetFiles(directory);
            foreach(var file in files)
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
        public void ReadPartitionDataTest()
        {
            #region #------------------------------- Assemble --------------------------------------#

            var directory = @"C:\Temp\Test\FP1\";

            //clean out the files
            var files = Directory.GetFiles(directory);
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

            for(int  i = 0; i < 12; i++)
            {
                var readItem = filePartitionerReader.ReadPartition(i);
                readRes.Add(readItem);
            }

            #endregion

            #region #------------------------------- Assert --------------------------------------#

            for(int i = 0; i < 12; i++)
            {
                Assert.NotNull(readRes[i]);
                Assert.Equal(PartitionActionStatus.ParitionStatusEnum.Success, readRes[i]!.Status);
                if(i < 11)
                    Assert.Equal(9, readRes[i]!.Data!.Count());
                else
                    Assert.Single(readRes[i]!.Data!);
            }

            #endregion
        }
    }



    public class TestData
    {
        private static Random random = new Random();

        public TestData() { }

        public TestData(int rowNumber)
        {
            RowNumber = rowNumber;
            NumberA = random.Next(0, 100_000);
            NumberB = random.Next(0, 100_000);
        }
        public int RowNumber { get; set; }
        public int NumberA { get; set; }
        public int NumberB { get; set; }
    }

    public class TestReadWrite : IFileReaderWriter<TestData>
    {
        public List<TestData> Read(string filepath)
        {
            return new List<TestData>();
        }

        public void Write(List<TestData> items, string filepath)
        {
            return;
        }
    }
}