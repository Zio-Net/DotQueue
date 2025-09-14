using Microsoft.Extensions.Logging;

namespace DotQueue;

public abstract class RoutedQueueHandler<TMessage, TAction> : IQueueHandler<TMessage>
    where TAction : notnull
{
    private readonly ILogger _logger;
    private readonly Dictionary<TAction, HandlerDelegate> _routes = new();

    protected RoutedQueueHandler(ILogger logger)
    {
        _logger = logger;
        Configure(new RouteBuilder(this));
    }

    protected abstract TAction GetAction(TMessage message);

    protected virtual void Configure(RouteBuilder routes) { }

    protected delegate Task HandlerDelegate(
        TMessage message,
        IReadOnlyDictionary<string, string>? metadata,
        Func<Task> renewLock,
        CancellationToken ct);

    protected void Register(TAction action, HandlerDelegate handler) => _routes[action] = handler;

    protected sealed class RouteBuilder
    {
        private readonly RoutedQueueHandler<TMessage, TAction> _owner;
        internal RouteBuilder(RoutedQueueHandler<TMessage, TAction> owner) => _owner = owner;

        public RouteBuilder On(TAction action, HandlerDelegate handler)
        {
            _owner.Register(action, handler);
            return this;
        }
    }

    public Task HandleAsync(
        TMessage message,
        IReadOnlyDictionary<string, string>? metadata,
        Func<Task> renewLock,
        CancellationToken ct)
    {
        var action = GetAction(message);

        if (_routes.TryGetValue(action, out var handler))
        {
            return handler(message, metadata, renewLock, ct);
        }

        _logger.LogWarning("No handler registered for action {Action}", action);
        throw new NonRetryableException($"No handler registered for action {action}");
    }
}