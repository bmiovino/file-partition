namespace FilePartitioner;


public interface IFileReaderWriter<T> where T : class, new()
{ 
    public List<T> Read(string filepath);
    public void Write(List<T> items, string filepath);
}
