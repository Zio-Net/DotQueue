using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DotQueue;
using DotQueue.Rabbit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Xunit;

namespace IntegrationTests;

public class DotQueue_Rabbit_Smoke
{
    private const string Amqp =
        "amqp://admin:admin@localhost:5673/";
    private const string TypedMessageContract = "integration-tests.rabbit-typed-msg.v1";

    [Fact(Timeout = 60000)]
    public async Task Message_is_received()
    {
        const string queueName = "demo-messages";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var gotIt = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(s =>
            {
                s.AddLogging(b => b.AddSimpleConsole(o =>
                {
                    o.TimestampFormat = "HH:mm:ss ";
                    o.SingleLine = true;
                }));

                s.AddSingleton(gotIt);

                s.AddRabbitMQQueue<SimpleMsg, SimpleHandler>(
                    amqpConnectionString: Amqp,
                    queueName: queueName,
                    configure: o =>
                    {
                        o.MaxConcurrentCalls = 1;
                        o.PrefetchCount = 0;
                        o.MaxRetryAttempts = 0;
                    });
            })
            .Build();

        await host.StartAsync(cts.Token);

        var factory = new ConnectionFactory { Uri = new Uri(Amqp) };
        await using var conn = await factory.CreateConnectionAsync(cts.Token);
        await using var ch = await conn.CreateChannelAsync(cancellationToken: cts.Token);

        for (var i = 0; i < 100; i++)
        {
            await using var probe = await conn.CreateChannelAsync(cancellationToken: cts.Token);
            try
            {
                var q = await probe.QueueDeclarePassiveAsync(queueName, cts.Token);
                if (q.ConsumerCount > 0) break;
            }
            catch { }
            await Task.Delay(100, cts.Token);
        }

        var payload = new SimpleMsg("hello");
        var body = JsonSerializer.SerializeToUtf8Bytes(payload);
        await ch.BasicPublishAsync("", queueName, false,
            new BasicProperties { ContentType = "application/json" }, body, cts.Token);

        var receivedText = await gotIt.Task.WaitAsync(cts.Token);
        Assert.Equal("hello", receivedText);

        await host.StopAsync(TimeSpan.FromSeconds(5));
    }

    [Fact(Timeout = 60000)]
    public async Task Typed_message_is_routed_by_messageType_header()
    {
        const string queueName = "demo-typed-messages";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await EnsureQueueExistsAsync(queueName, cts.Token);

        var gotIt = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(s =>
            {
                s.AddLogging(b => b.AddSimpleConsole(o =>
                {
                    o.TimestampFormat = "HH:mm:ss ";
                    o.SingleLine = true;
                }));

                s.AddSingleton(gotIt);

                s.AddTypedRoutedRabbitMQQueue<TypedHandler>(
                    amqpConnectionString: Amqp,
                    queueName: queueName,
                    configure: o =>
                    {
                        o.MaxConcurrentCalls = 1;
                        o.PrefetchCount = 0;
                        o.MaxRetryAttempts = 0;
                    });
            })
            .Build();

        await host.StartAsync(cts.Token);

        var factory = new ConnectionFactory { Uri = new Uri(Amqp) };
        await using var conn = await factory.CreateConnectionAsync(cts.Token);
        await using var ch = await conn.CreateChannelAsync(cancellationToken: cts.Token);

        for (var i = 0; i < 100; i++)
        {
            await using var probe = await conn.CreateChannelAsync(cancellationToken: cts.Token);
            try
            {
                var q = await probe.QueueDeclarePassiveAsync(queueName, cts.Token);
                if (q.ConsumerCount > 0) break;
            }
            catch { }
            await Task.Delay(100, cts.Token);
        }

        var body = JsonSerializer.SerializeToUtf8Bytes(new TypedMsg("typed-hello"));
        await ch.BasicPublishAsync(
            "",
            queueName,
            false,
            new BasicProperties
            {
                ContentType = "application/json",
                Headers = new Dictionary<string, object?>
                {
                    [TypedRoutedQueueHandler.MessageTypeMetadataKey] = Encoding.UTF8.GetBytes(TypedMessageContract),
                },
            },
            body,
            cts.Token);

        var receivedText = await gotIt.Task.WaitAsync(cts.Token);
        Assert.Equal("typed-hello", receivedText);

        await host.StopAsync(TimeSpan.FromSeconds(5));
    }

    private sealed record SimpleMsg(string Text);

    private sealed class SimpleHandler : IQueueHandler<SimpleMsg>
    {
        private readonly TaskCompletionSource<string> _tcs;
        private readonly ILogger<SimpleHandler> _log;

        public SimpleHandler(TaskCompletionSource<string> tcs, ILogger<SimpleHandler> log)
        {
            _tcs = tcs;
            _log = log;
        }

        public async Task HandleAsync(SimpleMsg message, IReadOnlyDictionary<string, string>? _, Func<Task> complete, CancellationToken ct)
        {
            _log.LogInformation("Got: {Text}", message.Text);
            await complete();
            _tcs.TrySetResult(message.Text);
        }
    }

    private sealed record TypedMsg(string Text);

    private sealed class TypedHandler : TypedRoutedQueueHandler
    {
        private readonly TaskCompletionSource<string> _tcs;

        public TypedHandler(TaskCompletionSource<string> tcs, ILogger<TypedHandler> logger) : base(logger)
        {
            _tcs = tcs;
            InitializeRoutes();
        }

        protected override RouteBuilder ConfigureRoutes(RouteBuilder routeBuilder)
            => routeBuilder.AddHandler<TypedMsg>(TypedMessageContract, HandleTypedAsync);

        private ValueTask HandleTypedAsync(
            TypedMsg message,
            IReadOnlyDictionary<string, string>? metadata,
            Func<Task> renewLock,
            CancellationToken ct)
        {
            _tcs.TrySetResult(message.Text);
            return ValueTask.CompletedTask;
        }
    }

    private static async Task EnsureQueueExistsAsync(string queueName, CancellationToken ct)
    {
        var factory = new ConnectionFactory { Uri = new Uri(Amqp) };
        await using var conn = await factory.CreateConnectionAsync(ct);
        await using var ch = await conn.CreateChannelAsync(cancellationToken: ct);

        await ch.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: ct);
    }
}
