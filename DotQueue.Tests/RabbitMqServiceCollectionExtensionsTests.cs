using DotQueue.Rabbit;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DotQueue.Tests;

public class RabbitMqServiceCollectionExtensionsTests
{
    private sealed class DummyHandler : IQueueHandler<string>
    {
        public Task HandleAsync(
            string message,
            IReadOnlyDictionary<string, string>? metadata,
            Func<Task> renewLock,
            CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class DummyTypedHandler : TypedRoutedQueueHandler
    {
        public DummyTypedHandler(ILogger<DummyTypedHandler> logger) : base(logger) => InitializeRoutes();

        protected override RouteBuilder ConfigureRoutes(RouteBuilder routeBuilder)
            => routeBuilder.AddHandler<DummyMessage>("tests.rabbit-dummy.v1", HandleAsync);

        private static ValueTask HandleAsync(
            DummyMessage message,
            IReadOnlyDictionary<string, string>? metadata,
            Func<Task> renewLock,
            CancellationToken ct) => ValueTask.CompletedTask;
    }

    private sealed record DummyMessage(string Text);

    [Fact]
    public void AddRabbitQueue_Registers_Default_Pipeline()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRabbitMQQueue<string, DummyHandler>(
            amqpConnectionString: "amqp://guest:guest@localhost:5672/",
            queueName: "queue-name");

        var sp = services.BuildServiceProvider();

        sp.GetService<IQueueListener<string>>().Should().NotBeNull();
        sp.GetServices<Microsoft.Extensions.Hosting.IHostedService>().Should().NotBeEmpty();
    }

    [Fact]
    public void AddTypedRoutedRabbitQueue_Registers_Raw_Pipeline()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddTypedRoutedRabbitMQQueue<DummyTypedHandler>(
            amqpConnectionString: "amqp://guest:guest@localhost:5672/",
            queueName: "typed-queue-name");

        var sp = services.BuildServiceProvider();

        sp.GetService<IQueueListener<RawQueueMessage>>().Should().NotBeNull();
        sp.GetServices<Microsoft.Extensions.Hosting.IHostedService>().Should().NotBeEmpty();

        using var scope = sp.CreateScope();
        scope.ServiceProvider.GetRequiredService<IQueueHandler<RawQueueMessage>>().Should().BeOfType<DummyTypedHandler>();
    }
}
