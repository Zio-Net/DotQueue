
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotQueue;
public class QueueProcessor<T> : BackgroundService
{
    private readonly IQueueListener<T> _listener;
    private readonly IServiceScopeFactory _scopeFactory;

    public QueueProcessor(IQueueListener<T> listener, IServiceScopeFactory scopeFactory)
    {
        _listener = listener;
        _scopeFactory = scopeFactory;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
    _listener.StartAsync(async (msg, metadata, renewLock, token) =>
    {
        using var scope = _scopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<IQueueHandler<T>>();
        await handler.HandleAsync(msg, metadata, renewLock, token);
    }, stoppingToken);
}
