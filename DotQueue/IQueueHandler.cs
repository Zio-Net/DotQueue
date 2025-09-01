namespace DotQueue;

public interface IQueueHandler<T>
{
    Task HandleAsync(T message, IReadOnlyDictionary<string, string>? metadataCallback, Func<Task> renewLock, CancellationToken cancellationToken);
}
