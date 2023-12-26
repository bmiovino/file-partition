using CsvHelper;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FilePartitioner;

public class CsvFileReaderWriter<T> : IFileReaderWriter<T> where T : class, new()
{
    public List<T> Read(string filepath)
    {
        using (var reader = new StreamReader(filepath))
        using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
        {
            return csv.GetRecords<T>().ToList();
        }
    }

    public void Write(List<T> items, string filepath)
    {
        using (var writer = new StreamWriter(filepath))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            csv.WriteRecords(items);
        }
    }
}
