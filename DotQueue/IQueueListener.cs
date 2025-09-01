namespace DotQueue;

public interface IQueueListener<T>
{
    Task StartAsync(Func<T, IReadOnlyDictionary<string, string>?, Func<Task>, CancellationToken, Task> handler, CancellationToken cancellationToken);
}
