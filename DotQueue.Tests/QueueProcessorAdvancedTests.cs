using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Xunit;

namespace DotQueue.Tests;

public class QueueProcessorAdvancedTests
{
    private class TestListener<T> : IQueueListener<T>
    {
        public Func<T, IReadOnlyDictionary<string, string>?, Func<Task>, CancellationToken, Task>? CapturedHandler { get; private set; }
        public Task StartAsync(Func<T, IReadOnlyDictionary<string, string>?, Func<Task>, CancellationToken, Task> handler, CancellationToken cancellationToken)
        {
            CapturedHandler = handler;
            return Task.CompletedTask;
        }
    }

    private sealed class ScopedResource : IDisposable
    {
        public bool Disposed { get; private set; }
        public Guid Id { get; } = Guid.NewGuid();
        public void Dispose() => Disposed = true;
    }

    private sealed class UsedResources
    {
        public List<ScopedResource> Items { get; } = new();
    }

    private sealed class TrackingHandler : IQueueHandler<string>
    {
        private readonly ScopedResource _resource;
        private readonly UsedResources _tracker;
        public TrackingHandler(ScopedResource resource, UsedResources tracker)
        {
            _resource = resource;
            _tracker = tracker;
        }

        public Task HandleAsync(string message, IReadOnlyDictionary<string, string>? metadata, Func<Task> renewLock, CancellationToken cancellationToken)
        {
            _tracker.Items.Add(_resource);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task NewScopePerMsg()
    {
        var services = new ServiceCollection();
        var listener = new TestListener<string>();
        services.AddSingleton<IQueueListener<string>>(listener);

        services.AddSingleton<UsedResources>();
        services.AddScoped<ScopedResource>();
        services.AddScoped<IQueueHandler<string>, TrackingHandler>();
        services.AddHostedService<QueueProcessor<string>>();

        using var sp = services.BuildServiceProvider();
        var hosted = sp.GetRequiredService<IHostedService>();

        await hosted.StartAsync(CancellationToken.None);

        await listener.CapturedHandler!("msg1", new Dictionary<string, string>(), () => Task.CompletedTask, CancellationToken.None);
        await listener.CapturedHandler!("msg2", new Dictionary<string, string>(), () => Task.CompletedTask, CancellationToken.None);

        await hosted.StopAsync(CancellationToken.None);

        var tracker = sp.GetRequiredService<UsedResources>();
        tracker.Items.Should().HaveCount(2);
        tracker.Items[0].Id.Should().NotBe(tracker.Items[1].Id);
        tracker.Items.Should().OnlyContain(r => r.Disposed);
    }

    private sealed class RenewLockHandler : IQueueHandler<string>
    {
        public int Calls { get; private set; }
        public Task HandleAsync(string message, IReadOnlyDictionary<string, string>? metadata, Func<Task> renewLock, CancellationToken cancellationToken)
        {
            renewLock().Wait();
            renewLock().Wait();
            Calls = 2;
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task RenewLockCallable()
    {
        var services = new ServiceCollection();
        var listener = new TestListener<string>();
        services.AddSingleton<IQueueListener<string>>(listener);

        var mock = new Moq.Mock<IQueueHandler<string>>();
        services.AddScoped<IQueueHandler<string>>(_ => mock.Object);

        services.AddHostedService<QueueProcessor<string>>();

        using var sp = services.BuildServiceProvider();
        var hosted = sp.GetRequiredService<IHostedService>();
        await hosted.StartAsync(CancellationToken.None);

        int renewCount = 0;
        Task Renew() { renewCount++; return Task.CompletedTask; }

        mock.Setup(h => h.HandleAsync("hello", new Dictionary<string, string>(), It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()))
            .Returns<string, IReadOnlyDictionary<string, string>?, Func<Task>, CancellationToken>(async (msg, meta, renew, ct) =>
            {
                await renew();
                await renew();
            });

        await listener.CapturedHandler!("hello", new Dictionary<string, string>(), Renew, CancellationToken.None);
        await hosted.StopAsync(CancellationToken.None);

        mock.Verify(h => h.HandleAsync("hello", new Dictionary<string, string>(), It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()), Times.Once);
        renewCount.Should().Be(2);
    }
}
