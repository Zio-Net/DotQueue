do# DotQueue
Provider‑agnostic .NET queue consumer + message pump. Includes Azure ServiceBus Queues adapter

## Typed routed handlers (agent-style)

Use `TypedRoutedQueueHandler` when each incoming message has its own CLR type and
the queue metadata contains a discriminator key `messageType`.

```csharp
using DotQueue;
using Microsoft.Extensions.Logging;

internal sealed class DecisionAgentQueueHandler(ILogger<DecisionAgentQueueHandler> logger)
    : TypedRoutedQueueHandler(logger)
{
    protected override RouteBuilder ConfigureRoutes(RouteBuilder routeBuilder)
        => routeBuilder
            .AddHandler<InterventionContextData>(CollectContextAsync)
            .AddHandler<DecisionAgentInput, InterventionDecisionResult>(MakeDecisionAsync);

    private static ValueTask CollectContextAsync(
        InterventionContextData message,
        IReadOnlyDictionary<string, string>? metadata,
        Func<Task> renewLock,
        CancellationToken ct)
    {
        // Collect context from one-way message.
        return ValueTask.CompletedTask;
    }

    private static ValueTask<InterventionDecisionResult> MakeDecisionAsync(
        DecisionAgentInput message,
        IReadOnlyDictionary<string, string>? metadata,
        Func<Task> renewLock,
        CancellationToken ct)
    {
        var result = new InterventionDecisionResult();
        return ValueTask.FromResult(result);
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

Set metadata/header key `messageType` to the target CLR type name (or full name),
and serialize the body as JSON for that type.

Azure Service Bus:

```csharp
var body = JsonSerializer.Serialize(new InterventionContextData { /* ... */ });
var message = new ServiceBusMessage(body);
message.ApplicationProperties["messageType"] = nameof(InterventionContextData);
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
            ["messageType"] = Encoding.UTF8.GetBytes(nameof(InterventionContextData))
        }
    },
    body: body);
```
