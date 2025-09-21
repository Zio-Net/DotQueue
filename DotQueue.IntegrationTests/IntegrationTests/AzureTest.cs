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

    private sealed record SimpleMsg(string Text);

    private sealed class SimpleHandler : IQueueHandler<SimpleMsg>
    {
        private readonly TaskCompletionSource<string> _tcs;
        private readonly ILogger<SimpleHandler> _log;

        public SimpleHandler(TaskCompletionSource<string> tcs, ILogger<SimpleHandler> log)
        {
            _tcs = tcs; _log = log;
        }

        public async Task HandleAsync(SimpleMsg message, IReadOnlyDictionary<string, string>? _, Func<Task> complete, CancellationToken ct)
        {
            _log.LogInformation("Got: {Text}", message.Text);
            if (complete is not null) await complete();   
            _tcs.TrySetResult(message.Text);             
        }
    }
}
