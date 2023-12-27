# file-partition
This project provides a way to partition large data sets into mulitiple, persisted to disk data files.  One file per partition is stored.

## Code Example
### Example Test Classes
```csharp
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
```
### Example Test Writer
```csharp
var directory = @"C:\Temp\Data_Directory\Data_Set_Type\";
var filebase = "data_set_type_name";
var extension = "csv";

//create a file partitioner of type Test Data, using the CSV reader writer implementation (based on CSVHelper)
FilePartitioner<TestData> filePartitioner = new FilePartitioner.FilePartitioner<TestData>(new CsvFileReaderWriter<TestData>(), directory, filebase, extension);

var data = new List<TestData>();
for(int i = 0; i < 100; i++)
    data.Add(new TestData(i));

var res = filePartitioner.WritePartitions(data, 9);
```

### Example Test Reader
```csharp
FilePartitioner<TestData> filePartitionerReader = new FilePartitioner.FilePartitioner<TestData>(new CsvFileReaderWriter<TestData>(), directory, filebase, extension);

var readRes = new List<PartitionReadResult<TestData>?>();

for(int  i = 0; i < 12; i++)
{
    var readItem = filePartitionerReader.ReadPartition(i);
    readRes.Add(readItem);
}
```
