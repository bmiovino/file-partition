using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FilePartitioner;

/// <summary>
/// Local disk persistent data partitioner.  For use in long running processes to 
/// enable a more graceful failure and resume when processing.  Partitioned data 
/// may be persisted for long periods of time, allowing for data set usage that
/// spans instances of processing.
/// </summary>
/// <typeparam name="T">Partition data item type.</typeparam>
public partial class FilePartitioner<T> where T : class, new()
{

    #region # Public Properties
    public int MinIndex { get; set; } = 0;
    public int MaxIndex { get; set; } = 0;
    public int NumberOfPartitions { get; set; } = 0;
    #endregion

    #region # Private Properties
    private IFileReaderWriter<T> _fileReaderWriter;
    private bool _universalBoundariesSet = false;
    private string _baseFileName;
    private string _baseDirectory;
    private string _fileExtension;
    private Dictionary<int, PartitionRecord> _partitionRecords = new();
    private Regex _partitionFileNameRegex = new Regex(@".*_(?<minindex>\d*)_(?<maxindex>\d*)[.].*", RegexOptions.Compiled | RegexOptions.Singleline);
    #endregion

    /// <summary>
    /// Constructor for instance of the file partitioner.
    /// </summary>
    /// <param name="fileReaderWriter">DI of file reader/writer implementation.</param>
    /// <param name="baseDirectory">Directory where the all the partitioned data is persisted on disk.</param>
    /// <param name="baseFileName">base file name for the partition data files (doesn't include the extension)</param>
    /// <param name="fileExtension">file extension for the parition data files</param>
    public FilePartitioner(IFileReaderWriter<T> fileReaderWriter, string baseDirectory, string baseFileName, string fileExtension)
    {
        _fileReaderWriter = fileReaderWriter;
        _baseFileName = baseFileName;
        _baseDirectory = baseDirectory;
        _fileExtension = fileExtension;
    }

    /// <summary>
    /// Constructor that implements the Csv Reader Writer based on CsvHelper.
    /// </summary>
    /// <param name="baseDirectory">Directory where the all the partitioned data is persisted on disk.</param>
    /// <param name="baseFileName">base file name for the partition data files (doesn't include the extension)</param>
    /// <param name="fileExtension">file extension for the parition data files</param>
    public FilePartitioner(string baseDirectory, string baseFileName, string fileExtension)
    {
        _fileReaderWriter = new CsvFileReaderWriter<T>();
        _baseFileName = baseFileName;
        _baseDirectory = baseDirectory;
        _fileExtension = fileExtension;
    }

    public PartitionActionStatus WriteSinglePartition(IEnumerable<T> data)
    {
        return WritePartitions(data, data.Count());
    }

    /// <summary>
    /// Write all partitions for a data set.  This also initializes this instance for reading partition data.
    /// </summary>
    /// <param name="data">Data set to partition according to the partitionSize</param>
    /// <param name="partitionSize"></param>
    /// <returns></returns>
    public PartitionActionStatus WritePartitions(IEnumerable<T> data, int partitionSize = 100_000)
    {
        var writeResult = new PartitionActionStatus();

        try
        {
            MinIndex = 0;
            MaxIndex = data.Count() - 1;
            NumberOfPartitions = (int)Math.Ceiling((double)data.Count() / partitionSize);

            for (int i = 0; i < NumberOfPartitions; i++)
            {
                var paritionMinIndex = i * partitionSize;
                var partitionMaxIndex = ((i + 1) * partitionSize) - 1;

                if (partitionMaxIndex > MaxIndex)
                    partitionMaxIndex = MaxIndex;

                var filePath = GetPartitionFilePath(paritionMinIndex, partitionMaxIndex);

                var partitionData = data.Skip(paritionMinIndex).Take(partitionMaxIndex - paritionMinIndex + 1).ToList();

                _fileReaderWriter.Write(partitionData, filePath);

                _partitionRecords.Add(i, new PartitionRecord {  MaxIndex = partitionMaxIndex, MinIndex = paritionMinIndex, Number = i });
            }

            _universalBoundariesSet = true;

            writeResult.Status = PartitionActionStatus.ParitionStatusEnum.Success;
        }
        catch (Exception e)
        {
            writeResult.Status = PartitionActionStatus.ParitionStatusEnum.Error;
            writeResult.Message = e.Message + " " + e.Source + " " + e.StackTrace;
        }

        return writeResult;
    }

    /// <summary>
    /// Read a partition number.  Zero based partition numbering.
    /// </summary>
    /// <param name="partitionNumber">Parition number to read.  Starting with 0.</param>
    /// <returns>A partition read result with status of Success or Error/w/message.  If success then the data is returned for the partition.</returns>
    public PartitionReadResult<T> ReadPartition(int partitionNumber)
    {
        if(!_universalBoundariesSet)
        {
            ScanBaseDirectory();
            _universalBoundariesSet = true;
        }

        if (partitionNumber < 0 || partitionNumber >= NumberOfPartitions)
            return new PartitionReadResult<T>(PartitionActionStatus.ParitionStatusEnum.Error, "Partition index out of range or invalid.");
        
        var partitionRecord = _partitionRecords[partitionNumber];
        var partitionRecordFilePath = GetPartitionFilePath(partitionRecord.MinIndex, partitionRecord.MaxIndex);
        var data = _fileReaderWriter.Read(partitionRecordFilePath);

        return new PartitionReadResult<T>(PartitionActionStatus.ParitionStatusEnum.Success, "") { Data = data, PartitionRecord = partitionRecord };
    }

    public string GetPartitionFilePath(int minIndex, int maxIndex)
    {
        return $"{_baseDirectory.TrimEnd('\\')}\\{_baseFileName}_{minIndex}_{maxIndex}.{_fileExtension}";
    }

    /// <summary>
    /// This can be run at anytime to refresh the partition records based on the data directory, file name base and extension.
    /// </summary>
    /// <exception cref="Exception"><inheritdoc cref="Exception"/></exception>
    private void ScanBaseDirectory()
    {
        var files = Directory.GetFiles(_baseDirectory, _baseFileName + "_*");

        var partitionRecords = new List<PartitionRecord>();

        foreach (var file in files)
        {
            var match = _partitionFileNameRegex.Match(file);

            if (match.Success)
            {
                var partitionRecord = new PartitionRecord();
                partitionRecord.MinIndex = int.Parse(match.Groups["minindex"].Value);
                partitionRecord.MaxIndex = int.Parse(match.Groups["maxindex"].Value);
                partitionRecords.Add(partitionRecord);
            }
        }

        if (partitionRecords.Count == 0)
            throw new Exception("No partition files were found.");

        int i = 0;
        partitionRecords = partitionRecords.OrderBy(i => i.MinIndex).ToList();
        partitionRecords.ForEach(r => { r.Number = i++; });

        _partitionRecords = partitionRecords.ToDictionary(i => i.Number, i => i);

        NumberOfPartitions = partitionRecords.Count;
        MinIndex = _partitionRecords[0].MinIndex;
        MaxIndex = _partitionRecords[_partitionRecords.Count - 1].MaxIndex;
    }
}
