using DotQueue;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DotQueue.Tests;

public class TypedRoutedQueueHandlerTests
{
    private sealed record ContextMessage(string Source);
    private sealed record DecisionInput(int Count);
    private sealed record DecisionResult(string Value);

    private sealed class TestTypedHandler : TypedRoutedQueueHandler
    {
        public int ContextCalls { get; private set; }
        public int DecisionCalls { get; private set; }
        public int RenewCalls { get; private set; }
        public IReadOnlyDictionary<string, string>? LastMetadata { get; private set; }
        public DecisionResult? LastResult { get; private set; }

        public TestTypedHandler(ILogger logger) : base(logger) { }

        protected override RouteBuilder ConfigureRoutes(RouteBuilder routeBuilder)
            => routeBuilder
                .AddHandler<ContextMessage>(HandleContextAsync)
                .AddHandler<DecisionInput, DecisionResult>(HandleDecisionAsync);

        private async ValueTask HandleContextAsync(
            ContextMessage message,
            IReadOnlyDictionary<string, string>? metadata,
            Func<Task> renewLock,
            CancellationToken ct)
        {
            LastMetadata = metadata;
            await renewLock();
            RenewCalls++;
            ContextCalls++;
        }

        private ValueTask<DecisionResult> HandleDecisionAsync(
            DecisionInput message,
            IReadOnlyDictionary<string, string>? metadata,
            Func<Task> renewLock,
            CancellationToken ct)
        {
            LastMetadata = metadata;
            DecisionCalls++;
            LastResult = new DecisionResult($"decision-{message.Count}");
            return ValueTask.FromResult(LastResult);
        }
    }

    private sealed class WrappedInvocationTypedHandler : TypedRoutedQueueHandler
    {
        public bool WasWrapped { get; private set; }
        public Type? LastType { get; private set; }
        public int Calls { get; private set; }

        public WrappedInvocationTypedHandler(ILogger logger) : base(logger) { }

        protected override RouteBuilder ConfigureRoutes(RouteBuilder routeBuilder)
            => routeBuilder.AddHandler<ContextMessage>(HandleContextAsync);

        protected override async ValueTask InvokeHandlerAsync(
            Type messageType,
            HandlerDelegate handler,
            object message,
            IReadOnlyDictionary<string, string>? metadata,
            Func<Task> renewLock,
            CancellationToken ct)
        {
            WasWrapped = true;
            LastType = messageType;
            await handler(message, metadata, renewLock, ct);
        }

        private ValueTask HandleContextAsync(
            ContextMessage message,
            IReadOnlyDictionary<string, string>? metadata,
            Func<Task> renewLock,
            CancellationToken ct)
        {
            Calls++;
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task Routes_By_MessageType_And_Passes_Metadata_And_RenewLock()
    {
        var logger = Mock.Of<ILogger>();
        var handler = new TestTypedHandler(logger);

        var metadata = new Dictionary<string, string>
        {
            [TypedRoutedQueueHandler.MessageTypeMetadataKey] = nameof(ContextMessage),
            ["traceId"] = "t1",
        };

        var renewCount = 0;
        Task Renew()
        {
            renewCount++;
            return Task.CompletedTask;
        }

        await handler.HandleAsync(
            new RawQueueMessage("""{"source":"student-feed"}"""),
            metadata,
            Renew,
            CancellationToken.None);

        handler.ContextCalls.Should().Be(1);
        handler.DecisionCalls.Should().Be(0);
        handler.RenewCalls.Should().Be(1);
        renewCount.Should().Be(1);
        handler.LastMetadata.Should().NotBeNull();
        handler.LastMetadata!["traceId"].Should().Be("t1");
    }

    [Fact]
    public async Task AddHandler_With_Response_Executes_And_Result_Is_Created()
    {
        var logger = Mock.Of<ILogger>();
        var handler = new TestTypedHandler(logger);

        var metadata = new Dictionary<string, string>
        {
            [TypedRoutedQueueHandler.MessageTypeMetadataKey] = "decisioninput",
        };

        await handler.HandleAsync(
            new RawQueueMessage("""{"count":3}"""),
            metadata,
            () => Task.CompletedTask,
            CancellationToken.None);

        handler.DecisionCalls.Should().Be(1);
        handler.LastResult.Should().NotBeNull();
        handler.LastResult!.Value.Should().Be("decision-3");
    }

    [Fact]
    public async Task FullName_MessageType_Is_Also_Supported()
    {
        var logger = Mock.Of<ILogger>();
        var handler = new TestTypedHandler(logger);

        var metadata = new Dictionary<string, string>
        {
            [TypedRoutedQueueHandler.MessageTypeMetadataKey] = typeof(ContextMessage).FullName!,
        };

        await handler.HandleAsync(
            new RawQueueMessage("""{"source":"full-name"}"""),
            metadata,
            () => Task.CompletedTask,
            CancellationToken.None);

        handler.ContextCalls.Should().Be(1);
    }

    [Fact]
    public async Task Missing_MessageType_Is_NonRetryable()
    {
        var logger = Mock.Of<ILogger>();
        var handler = new TestTypedHandler(logger);

        var act = () => handler.HandleAsync(
            new RawQueueMessage("""{"source":"x"}"""),
            new Dictionary<string, string>(),
            () => Task.CompletedTask,
            CancellationToken.None);

        await act.Should().ThrowAsync<NonRetryableException>()
            .WithMessage("*Missing required metadata key*");
    }

    [Fact]
    public async Task Unknown_MessageType_Is_NonRetryable()
    {
        var logger = Mock.Of<ILogger>();
        var handler = new TestTypedHandler(logger);

        var act = () => handler.HandleAsync(
            new RawQueueMessage("""{"source":"x"}"""),
            new Dictionary<string, string>
            {
                [TypedRoutedQueueHandler.MessageTypeMetadataKey] = "UnknownMessage",
            },
            () => Task.CompletedTask,
            CancellationToken.None);

        await act.Should().ThrowAsync<NonRetryableException>()
            .WithMessage("*No handler registered for messageType*");
    }

    [Fact]
    public async Task Invalid_Json_Is_NonRetryable()
    {
        var logger = Mock.Of<ILogger>();
        var handler = new TestTypedHandler(logger);

        var act = () => handler.HandleAsync(
            new RawQueueMessage("""{"source":}"""),
            new Dictionary<string, string>
            {
                [TypedRoutedQueueHandler.MessageTypeMetadataKey] = nameof(ContextMessage),
            },
            () => Task.CompletedTask,
            CancellationToken.None);

        await act.Should().ThrowAsync<NonRetryableException>()
            .WithMessage("*Failed to deserialize*");
    }

    [Fact]
    public async Task HandleAsync_Uses_InvokeHandlerAsync_For_Resolved_Types()
    {
        var logger = Mock.Of<ILogger>();
        var handler = new WrappedInvocationTypedHandler(logger);

        await handler.HandleAsync(
            new RawQueueMessage("""{"source":"wrapped"}"""),
            new Dictionary<string, string>
            {
                [TypedRoutedQueueHandler.MessageTypeMetadataKey] = nameof(ContextMessage),
            },
            () => Task.CompletedTask,
            CancellationToken.None);

        handler.WasWrapped.Should().BeTrue();
        handler.LastType.Should().Be(typeof(ContextMessage));
        handler.Calls.Should().Be(1);
    }
}
