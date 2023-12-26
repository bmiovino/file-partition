using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FilePartitioner;

public partial class FilePartitioner<T> where T : class, new()
{
    private IFileReaderWriter<T> _fileReaderWriter;
    public int MinIndex { get; set; } = 0;
    public int MaxIndex { get; set; } = 0;
    public int NumberOfPartitions { get; set; } = 0;    
    private bool _universalBoundariesSet = false;
    private string _baseFileName;
    private string _baseDirectory;
    private string _fileExtension;
    private Dictionary<int, PartitionRecord> _partitionRecords = new();

    public FilePartitioner(IFileReaderWriter<T> fileReaderWriter, string baseDirectory, string baseFileName, string fileExtension)
    {
        _fileReaderWriter = fileReaderWriter;
        _baseFileName = baseFileName;
        _baseDirectory = baseDirectory;
        _fileExtension = fileExtension;
    }

    public PartitionActionStatus WriteSinglePartition(IEnumerable<T> data)
    {
        return WritePartitions(data, data.Count());
    }

    public PartitionActionStatus WritePartitions(IEnumerable<T> data, int boundarySize = 100_000)
    {
        var writeResult = new PartitionActionStatus();

        try
        {
            MinIndex = 0;
            MaxIndex = data.Count() - 1;
            NumberOfPartitions = (int)Math.Ceiling((double)data.Count() / boundarySize);

            for (int i = 0; i < NumberOfPartitions; i++)
            {
                var paritionMinIndex = i * boundarySize;
                var partitionMaxIndex = ((i + 1) * boundarySize) - 1;

                if (partitionMaxIndex > MaxIndex)
                    partitionMaxIndex = MaxIndex;

                var filePath = GetPartitionFilePath(paritionMinIndex, partitionMaxIndex);

                _fileReaderWriter.Write(data.Skip(boundarySize).Take(boundarySize).ToList(), filePath);

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

        return new PartitionReadResult<T>(PartitionActionStatus.ParitionStatusEnum.Success, "") { Data = data };
    }

    public string GetPartitionFilePath(int minIndex, int maxIndex)
    {
        return $"{_baseDirectory.TrimEnd('\\')}\\{_baseFileName}_{minIndex}_{maxIndex}.{_fileExtension}";
    }

    private Regex _partitionFileNameRegex = new Regex(@".*_(?<minindex>\d*)_(?<maxindex>\d*)[.].*", RegexOptions.Compiled | RegexOptions.Singleline);

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

        MinIndex = _partitionRecords[0].MinIndex;
        MaxIndex = _partitionRecords[_partitionRecords.Count - 1].MaxIndex;
    }
}
