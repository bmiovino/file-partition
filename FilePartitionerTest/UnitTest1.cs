
using FilePartitioner;

namespace FilePartitionerTest
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {
            var directory = @"C:\Temp\Test\FP1\";
            var filebase = "file";
            FilePartitioner.FilePartitioner<TestData> filePartitioner = new FilePartitioner.FilePartitioner<TestData>(new TestReadWrite(), directory, filebase);
            filePartitioner.
        }
    }

    public class TestData
    {

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