namespace DotQueue;

public interface IQueueListener<T>
{
    Task StartAsync(Func<T, Func<Task>, CancellationToken, Task> handler, CancellationToken cancellationToken);
}
