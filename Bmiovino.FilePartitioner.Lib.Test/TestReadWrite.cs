
using FilePartitioner;

namespace FilePartitionerTest
{
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