using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DotQueue;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DotQueue.Tests;

public class RoutedQueueHandlerTests
{
    private enum Act { A, B }

    private sealed class TestMessage
    {
        public Act Action { get; init; }
    }

    private sealed class TestHandler : RoutedQueueHandler<TestMessage, Act>
    {
        public int ACalls { get; private set; }
        public int BCalls { get; private set; }
        public int RenewCalls { get; private set; }
        public IReadOnlyDictionary<string, string>? LastMetadata { get; private set; }

        public TestHandler(ILogger logger) : base(logger) { }

        protected override Act GetAction(TestMessage message) => message.Action;

        protected override void Configure(RouteBuilder r) => r
            .On(Act.A, HandleA)
            .On(Act.B, HandleB);

        private Task HandleA(
            TestMessage m,
            IReadOnlyDictionary<string, string>? meta,
            Func<Task> renew,
            CancellationToken ct)
        {
            LastMetadata = meta;
            ACalls++;
            return Task.CompletedTask;
        }

        private async Task HandleB(
            TestMessage m,
            IReadOnlyDictionary<string, string>? meta,
            Func<Task> renew,
            CancellationToken ct)
        {
            LastMetadata = meta;
            await renew();
            RenewCalls++;
            BCalls++;
        }
    }

    [Fact]
    public async Task Routes_To_Registered_Handlers_And_Passes_Metadata_And_RenewLock()
    {
        var logger = Mock.Of<ILogger>();
        var handler = new TestHandler(logger);

        var meta = new Dictionary<string, string> { ["k"] = "v" };
        int renewCount = 0;
        Task Renew() { renewCount++; return Task.CompletedTask; }

        await handler.HandleAsync(new TestMessage { Action = Act.A }, meta, Renew, CancellationToken.None);
        await handler.HandleAsync(new TestMessage { Action = Act.B }, meta, Renew, CancellationToken.None);

        handler.ACalls.Should().Be(1);
        handler.BCalls.Should().Be(1);
        handler.RenewCalls.Should().Be(1);
        handler.LastMetadata.Should().NotBeNull();
        handler.LastMetadata!["k"].Should().Be("v");
        renewCount.Should().Be(1);
    }

    [Fact]
    public async Task Unknown_Action_Is_NonRetryable_From_DotQueue_Base()
    {
        var logger = Mock.Of<ILogger>();
        var handler = new TestHandler(logger);

        var act = () => handler.HandleAsync(
            new TestMessage { Action = (Act)1234 }, // not registered
            null,
            () => Task.CompletedTask,
            CancellationToken.None);

        await act.Should().ThrowAsync<NonRetryableException>()
                 .WithMessage("*No handler registered for action*");
    }
}
