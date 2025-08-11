namespace DotQueue;

public interface IQueueHandler<T>
{
    Task HandleAsync(T message, Func<Task> renewLock, CancellationToken cancellationToken);
}
