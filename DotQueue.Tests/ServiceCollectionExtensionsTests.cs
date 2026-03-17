using Azure.Messaging.ServiceBus;
using DotQueue.Azure;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DotQueue.Tests;

public class ServiceCollectionExtensionsTests
{
    private class DummyHandler : IQueueHandler<string>
    {
        public Task HandleAsync(string message, IReadOnlyDictionary<string, string>? metadata, Func<Task> renewLock, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class DummyTypedRoutedHandler : TypedRoutedQueueHandler
    {
        public DummyTypedRoutedHandler(ILogger<DummyTypedRoutedHandler> logger) : base(logger) => InitializeRoutes();

        protected override RouteBuilder ConfigureRoutes(RouteBuilder routeBuilder)
            => routeBuilder.AddHandler<DummyTypedMessage>("tests.dummy-typed.v1", HandleAsync);

        private static ValueTask HandleAsync(
            DummyTypedMessage message,
            IReadOnlyDictionary<string, string>? metadata,
            Func<Task> renewLock,
            CancellationToken ct) => ValueTask.CompletedTask;
    }

    private sealed record DummyTypedMessage(string Value);

    [Fact]
    public void AddQueueServiceCollectionTest()
    {
        var services = new ServiceCollection();

        services.AddSingleton(new ServiceBusClient("Endpoint=sb://localhost/;SharedAccessKeyName=Dummy;SharedAccessKey=Dummy"));
        services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();
        services.AddLogging();

        services.AddQueue<string, DummyHandler>("queue-name", s =>
        {
            s.MaxConcurrentCalls = 2;
            s.PrefetchCount = 10;
            s.MaxRetryAttempts = 5;
            s.RetryDelaySeconds = 1;
        });

        var sp = services.BuildServiceProvider();

        var listener = sp.GetService<IQueueListener<string>>();
        listener.Should().NotBeNull();

        var hosted = sp.GetServices<Microsoft.Extensions.Hosting.IHostedService>();
        hosted.Should().NotBeEmpty("QueueProcessor is added as hosted service");

        using var scope = sp.CreateScope();
        scope.ServiceProvider.GetRequiredService<IQueueHandler<string>>().Should().NotBeNull();
    }

    [Fact]
    public void AddTypedRoutedQueue_Registers_Raw_Listener_And_Handler()
    {
        var services = new ServiceCollection();

        services.AddSingleton(new ServiceBusClient("Endpoint=sb://localhost/;SharedAccessKeyName=Dummy;SharedAccessKey=Dummy"));
        services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();
        services.AddLogging();

        services.AddTypedRoutedQueue<DummyTypedRoutedHandler>("typed-queue-name");

        var sp = services.BuildServiceProvider();

        var listener = sp.GetService<IQueueListener<RawQueueMessage>>();
        listener.Should().NotBeNull();

        var hosted = sp.GetServices<Microsoft.Extensions.Hosting.IHostedService>();
        hosted.Should().NotBeEmpty();

        using var scope = sp.CreateScope();
        scope.ServiceProvider.GetRequiredService<IQueueHandler<RawQueueMessage>>().Should().BeOfType<DummyTypedRoutedHandler>();
    }

    [Fact]
    public void AddTypedRoutedSessionQueue_Registers_Raw_Listener_And_Handler()
    {
        var services = new ServiceCollection();

        services.AddSingleton(new ServiceBusClient("Endpoint=sb://localhost/;SharedAccessKeyName=Dummy;SharedAccessKey=Dummy"));
        services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();
        services.AddLogging();

        services.AddTypedRoutedSessionQueue<DummyTypedRoutedHandler>("typed-session-queue");

        var sp = services.BuildServiceProvider();

        var listener = sp.GetService<IQueueListener<RawQueueMessage>>();
        listener.Should().NotBeNull();

        var hosted = sp.GetServices<Microsoft.Extensions.Hosting.IHostedService>();
        hosted.Should().NotBeEmpty();

        using var scope = sp.CreateScope();
        scope.ServiceProvider.GetRequiredService<IQueueHandler<RawQueueMessage>>().Should().BeOfType<DummyTypedRoutedHandler>();
    }
}
