namespace FilePartitionerTest
{
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
}