# DotQueue
Provider‑agnostic .NET queue consumer + message pump. Includes Azure ServiceBus Queues adapter

## Typed routed handlers (agent-style)

Use `TypedRoutedQueueHandler` when each incoming message has its own CLR type and
the queue metadata contains a discriminator key `messageType`.

```csharp
using DotQueue;
using Microsoft.Extensions.Logging;

internal sealed class DecisionAgentQueueHandler : TypedRoutedQueueHandler
{
    public DecisionAgentQueueHandler(ILogger<DecisionAgentQueueHandler> logger) : base(logger)
    {
        // Important: initialize routes after derived type construction is complete.
        InitializeRoutes();
    }

    private const string InterventionContextV1 = "interventions.context.v1";
    private const string DecisionInputV1 = "interventions.decision-input.v1";

    protected override RouteBuilder ConfigureRoutes(RouteBuilder routeBuilder)
        => routeBuilder
            .AddHandler<InterventionContextData>(InterventionContextV1, CollectContextAsync)
            .AddHandler<DecisionAgentInput>(DecisionInputV1, MakeDecisionAsync);

    private static ValueTask CollectContextAsync(
        InterventionContextData message,
        IReadOnlyDictionary<string, string>? metadata,
        Func<Task> renewLock,
        CancellationToken ct)
    {
        // Collect context from one-way message.
        return ValueTask.CompletedTask;
    }

    private static ValueTask MakeDecisionAsync(
        DecisionAgentInput message,
        IReadOnlyDictionary<string, string>? metadata,
        Func<Task> renewLock,
        CancellationToken ct)
    {
        // Trigger next step through your own publisher/event bus if needed.
        return ValueTask.CompletedTask;
    }
}
```

### DI registration

Azure queue:

```csharp
services.AddTypedRoutedQueue<DecisionAgentQueueHandler>("interventions");
```

Azure session queue:

```csharp
services.AddTypedRoutedSessionQueue<DecisionAgentQueueHandler>("interventions");
```

RabbitMQ queue:

```csharp
services.AddTypedRoutedRabbitMQQueue<DecisionAgentQueueHandler>(
    "amqp://user:pass@localhost:5672/",
    "interventions");
```

### Producer metadata contract

Set metadata/header key `messageType` to a stable contract key
(for example `interventions.context.v1`), then serialize the body as JSON.

Azure Service Bus:

```csharp
var body = JsonSerializer.Serialize(new InterventionContextData { /* ... */ });
var message = new ServiceBusMessage(body);
message.ApplicationProperties["messageType"] = "interventions.context.v1";
await sender.SendMessageAsync(message);
```

RabbitMQ:

```csharp
var body = JsonSerializer.SerializeToUtf8Bytes(new InterventionContextData { /* ... */ });
await channel.BasicPublishAsync(
    exchange: "",
    routingKey: "interventions",
    mandatory: false,
    basicProperties: new BasicProperties
    {
        Headers = new Dictionary<string, object?>
        {
            ["messageType"] = Encoding.UTF8.GetBytes("interventions.context.v1")
        }
    },
    body: body);
```
