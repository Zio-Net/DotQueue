using Azure.Messaging.ServiceBus;
using DotQueue;
using DotQueue.Azure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace IntegrationTests;
public class DotQueue_Smoke
{
    private const string Conn =
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
    private const string TypedMessageContract = "integration-tests.typed-msg.v1";

    [Fact(Timeout = 60000)]
    public async Task Message_is_received()
    {
        const string queueName = "demo-messages";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var gotIt = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(s =>
            {
                s.AddLogging(b => b.AddSimpleConsole(o => { o.TimestampFormat = "HH:mm:ss "; o.SingleLine = true; }));

                s.AddSingleton(_ => new ServiceBusClient(Conn, new ServiceBusClientOptions
                {
                    TransportType = ServiceBusTransportType.AmqpTcp
                }));

                s.AddSingleton(gotIt);

                s.AddQueue<SimpleMsg, SimpleHandler>(queueName, o =>
                {
                    o.MaxConcurrentCalls = 1;
                    o.PrefetchCount = 0;
                    o.MaxRetryAttempts = 0;
                });
            })
            .Build();

        await host.StartAsync(cts.Token);
        await Task.Delay(200, cts.Token);

        var client = host.Services.GetRequiredService<ServiceBusClient>();
        var sender = client.CreateSender(queueName);

        var payload = new SimpleMsg("hello");
        var body = JsonSerializer.Serialize(payload);

        await sender.SendMessageAsync(
            new ServiceBusMessage(body) { ContentType = "application/json" },
            cts.Token);

        var receivedText = await gotIt.Task.WaitAsync(cts.Token);
        Assert.Equal("hello", receivedText);

        await sender.DisposeAsync();
        await host.StopAsync(TimeSpan.FromSeconds(5));
    }

    [Fact(Timeout = 60000)]
    public async Task Typed_message_is_routed_by_messageType_metadata()
    {
        const string queueName = "demo-messages";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var gotIt = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(s =>
            {
                s.AddLogging(b => b.AddSimpleConsole(o => { o.TimestampFormat = "HH:mm:ss "; o.SingleLine = true; }));

                s.AddSingleton(_ => new ServiceBusClient(Conn, new ServiceBusClientOptions
                {
                    TransportType = ServiceBusTransportType.AmqpTcp
                }));

                s.AddSingleton(gotIt);

                s.AddTypedRoutedQueue<TypedHandler>(queueName, o =>
                {
                    o.MaxConcurrentCalls = 1;
                    o.PrefetchCount = 0;
                    o.MaxRetryAttempts = 0;
                });
            })
            .Build();

        await host.StartAsync(cts.Token);
        await Task.Delay(200, cts.Token);

        var client = host.Services.GetRequiredService<ServiceBusClient>();
        var sender = client.CreateSender(queueName);
        var body = JsonSerializer.Serialize(new TypedMsg("typed-hello"));

        var message = new ServiceBusMessage(body)
        {
            ContentType = "application/json",
        };
        message.ApplicationProperties[TypedRoutedQueueHandler.MessageTypeMetadataKey] = TypedMessageContract;

        await sender.SendMessageAsync(message, cts.Token);

        var receivedText = await gotIt.Task.WaitAsync(cts.Token);
        Assert.Equal("typed-hello", receivedText);

        await sender.DisposeAsync();
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

}
