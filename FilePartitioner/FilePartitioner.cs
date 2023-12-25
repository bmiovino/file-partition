using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
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
    private Dictionary<int, PartitionRecord> _partitionRecords = new();

    public FilePartitioner(IFileReaderWriter<T> fileReaderWriter, string baseFileName, string baseDirectory)
    {
        _fileReaderWriter = fileReaderWriter;
        _baseFileName = baseFileName;
        _baseDirectory = baseDirectory;
    }

    public PartitionActionStatus SetBoundaries(int minIndex, int maxIndex)
    {
        var res = new PartitionActionStatus();

        if (minIndex >= 0 && maxIndex >= 0 && minIndex <= maxIndex)
        {
            MinIndex = minIndex;
            MaxIndex = maxIndex;
            res.Status = PartitionActionStatus.ParitionStatusEnum.Success;
            return res;
        }

        res.Status = PartitionActionStatus.ParitionStatusEnum.Error;
        res.Message = $"Boundary indices are invalid : min-index: {minIndex}, max-index: {maxIndex}";
        return res;
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
            var res = SetBoundaries(0, data.Count());
            if (res.Status != PartitionActionStatus.ParitionStatusEnum.Success)
                throw new Exception(res.Message);

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
        }
        catch (Exception e)
        {
            writeResult.Status = PartitionActionStatus.ParitionStatusEnum.Error;
            writeResult.Message = e.Message + " " + e.Source + " " + e.StackTrace;
        }

        return writeResult;
    }

    public PartitionActionStatus ReadPartition(int partitionNumber)
    {
        if(!_universalBoundariesSet)
        {
            ScanBaseDirectory();
        }

    }

    public string GetPartitionFilePath(int minIndex, int maxIndex)
    {
        var parts = _baseFileName.Split(".");

        if (parts.Length > 2)
            throw new Exception("Only supports one dot in the file name before the extension.");

        return $"{parts[0]}_{minIndex}_{maxIndex}.{parts[1]}";
    }

    private void ScanBaseDirectory()
    {

    }
}
