# DotQueue
Provider‑agnostic .NET queue consumer + message pump. Includes Azure ServiceBus Queues adapter

## Azure queue telemetry

Both Azure listeners emit a `System.Diagnostics.ActivitySource` and a
`System.Diagnostics.Metrics.Meter` named `DotQueue`. No OpenTelemetry package is
required by this library. Subscribe using your application's telemetry setup:

```csharp
services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddSource("DotQueue"))
    .WithMetrics(metrics => metrics.AddMeter("DotQueue"));
```

Add exporters in the application as usual. Without subscribers, the listener
creates no delivery telemetry objects, spans, tag lists or timing measurements.
Capacity and current occupancy are kept per listener so a metrics subscriber can
attach later. Occupancy uses two atomic operations per delivery even without
subscribers; no delivery telemetry is allocated on that path.
RabbitMQ telemetry is outside this implementation.

One consumer span covers each broker delivery, including all in-process retries
and settlement. A valid W3C `traceparent` and optional `tracestate` application
property continue the publisher's trace as a remote parent. Missing or malformed
context starts a new trace rather than adopting an unrelated ambient activity.
The handler and its logs run under that consumer span.

Spans carry the queue name, message ID, broker delivery count and enqueue time,
queue wait in seconds, retry count and final outcome. Payloads and exception
messages are not added to telemetry. Final failures mark the span as an error;
a successful retry remains successful. Applications that currently create their
own consumer span should keep only their handler child span.

| Instrument | Unit | Meaning |
|---|---|---|
| `dotqueue.message.wait` | seconds | Broker enqueue time to the first handler invocation, including prefetch wait. Negative clock skew is clamped to zero. |
| `messaging.process.duration` | seconds | Total time inside handler attempts and configured processing delays for one delivery. Excludes retry backoff and settlement. |
| `dotqueue.handler.active` | handlers | Observable up-down counter of currently occupied delivery slots, including retry backoff and settlement. |
| `dotqueue.handler.limit` | handlers | Observable gauge of configured capacity for running listeners. Removed on disposal or failed start. |
| `dotqueue.message.settled` | messages | One final outcome per delivery. |
| `dotqueue.retry.attempts` | retries | Handler invocations after the first attempt, independent of broker redelivery count. |
| `dotqueue.lock.renewals` | renewals | Successful explicit handler lock renewals. SDK background auto-renewals are not included. |

Every metric has `messaging.destination.name`. Wait, processing, outcome, retry
and renewal metrics also have `messageType` when the application property is a
nonempty string. Use stable contract keys, never per-message values. Active and
limit metrics share queue-only dimensions so they can be compared directly.
Message, session, tenant and user IDs are never metric labels.

Limit is summed when multiple listeners consume the same queue in one process.
For session queues it is `MaxConcurrentSessions`, with the SDK default of one
concurrent call per session. Across replicas, compare the summed active handlers
with the summed limits.

Outcome values are `completed`, `abandoned`, `dead-lettered`, `lock-expired` and
`settlement-failed`. A settlement is counted only after the broker operation
succeeds. Message or session lock loss is reported as `lock-expired` without
attempting another settlement. Other settlement errors are reported as
`settlement-failed` and propagate to the SDK error callback. Null or invalid JSON
payloads are dead-lettered without invoking the handler, so they have no wait or
processing sample. Cancellation from the SDK reaches the handler token; a
successful abandonment remains `abandoned`, rather than claiming lock expiry.

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
