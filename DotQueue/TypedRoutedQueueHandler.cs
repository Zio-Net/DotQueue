using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace DotQueue;

public abstract class TypedRoutedQueueHandler : IQueueHandler<RawQueueMessage>
{
    public const string MessageTypeMetadataKey = "messageType";

    private readonly ILogger _logger;
    private readonly Dictionary<string, TypeRoute> _routesByMessageType = new(StringComparer.OrdinalIgnoreCase);
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private int _isInitialized;

    protected TypedRoutedQueueHandler(ILogger logger) => _logger = logger;

    /// <summary>
    /// Initializes routes once. Call this from the derived constructor after the derived
    /// type has finished its own initialization.
    /// </summary>
    protected void InitializeRoutes()
    {
        if (Interlocked.Exchange(ref _isInitialized, 1) == 1)
        {
            return;
        }

        ConfigureRoutes(new RouteBuilder(this));
    }

    protected abstract RouteBuilder ConfigureRoutes(RouteBuilder routeBuilder);

    protected delegate ValueTask HandlerDelegate(
        object message,
        IReadOnlyDictionary<string, string>? metadata,
        Func<Task> renewLock,
        CancellationToken ct);

    protected virtual ValueTask InvokeHandlerAsync(
        Type messageType,
        HandlerDelegate handler,
        object message,
        IReadOnlyDictionary<string, string>? metadata,
        Func<Task> renewLock,
        CancellationToken ct) => handler(message, metadata, renewLock, ct);

    private void Register(string contractKey, Type messageType, HandlerDelegate handler)
    {
        if (string.IsNullOrWhiteSpace(contractKey))
        {
            throw new ArgumentException("Contract key cannot be null or whitespace.", nameof(contractKey));
        }

        var route = new TypeRoute(messageType, handler);
        _routesByMessageType[contractKey] = route;
    }

    protected sealed class RouteBuilder
    {
        private readonly TypedRoutedQueueHandler _owner;

        internal RouteBuilder(TypedRoutedQueueHandler owner) => _owner = owner;

        public RouteBuilder AddHandler<TIn>(
            string contractKey,
            Func<TIn, IReadOnlyDictionary<string, string>?, Func<Task>, CancellationToken, ValueTask> handler)
        {
            _owner.Register(
                contractKey,
                typeof(TIn),
                (message, metadata, renewLock, ct) => handler((TIn)message, metadata, renewLock, ct));
            return this;
        }
    }

    public async Task HandleAsync(
        RawQueueMessage message,
        IReadOnlyDictionary<string, string>? metadata,
        Func<Task> renewLock,
        CancellationToken ct)
    {
        if (Volatile.Read(ref _isInitialized) == 0)
        {
            throw new InvalidOperationException(
                $"{GetType().Name} routes are not initialized. Call {nameof(InitializeRoutes)}() from the derived constructor.");
        }

        if (metadata is null || !TryGetMessageType(metadata, out var messageType))
        {
            _logger.LogWarning("Missing required metadata key {MetadataKey}", MessageTypeMetadataKey);
            throw new NonRetryableException($"Missing required metadata key '{MessageTypeMetadataKey}'.");
        }

        if (!_routesByMessageType.TryGetValue(messageType, out var route))
        {
            _logger.LogWarning("No handler registered for messageType {MessageType}", messageType);
            throw new NonRetryableException($"No handler registered for messageType '{messageType}'.");
        }

        object typedMessage;
        try
        {
            typedMessage = JsonSerializer.Deserialize(message.Body, route.MessageType, _jsonOptions)
                ?? throw new NonRetryableException($"Failed to deserialize messageType '{messageType}'.");
        }
        catch (JsonException jex)
        {
            _logger.LogWarning(jex, "Failed to deserialize messageType {MessageType}", messageType);
            throw new NonRetryableException($"Failed to deserialize messageType '{messageType}'.", jex);
        }

        await InvokeHandlerAsync(route.MessageType, route.Handler, typedMessage, metadata, renewLock, ct);
    }

    private static bool TryGetMessageType(IReadOnlyDictionary<string, string> metadata, out string messageType)
    {
        if (metadata.TryGetValue(MessageTypeMetadataKey, out var direct) && !string.IsNullOrWhiteSpace(direct))
        {
            messageType = direct;
            return true;
        }

        foreach (var (key, value) in metadata)
        {
            if (!key.Equals(MessageTypeMetadataKey, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(value))
            {
                messageType = value;
                return true;
            }

            break;
        }

        messageType = string.Empty;
        return false;
    }

    private sealed record TypeRoute(Type MessageType, HandlerDelegate Handler);
}
