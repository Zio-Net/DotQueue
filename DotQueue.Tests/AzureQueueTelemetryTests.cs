using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Azure.Messaging.ServiceBus;
using DotQueue.Azure;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DotQueue.Tests;

[CollectionDefinition("Queue telemetry", DisableParallelization = true)]
public class QueueTelemetryCollection;

[Collection("Queue telemetry")]
public class AzureQueueTelemetryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompletionContinuesRemoteTraceAndRecordsBrokerWait(bool session)
    {
        using var capture = new TelemetryCapture();
        var parent = ActivityContext.Parse("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01", "vendor=value");
        var message = CreateMessage(properties: new Dictionary<string, object>
        {
            ["traceparent"] = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
            ["tracestate"] = "vendor=value",
            ["messageType"] = "answers.assess.v1",
        });
        await using var pump = new TestPump(session, message);
        Activity? current = null;
        await pump.StartAsync((_, _, _, _) =>
        {
            current = Activity.Current;
            return Task.CompletedTask;
        });
        await pump.DeliverAsync();

        var span = Assert.Single(capture.Spans);
        Assert.Same(span, current);
        Assert.Equal(ActivityKind.Consumer, span.Kind);
        Assert.Equal(parent.TraceId, span.TraceId);
        Assert.Equal(parent.SpanId, span.ParentSpanId);
        Assert.Equal("vendor=value", span.TraceStateString);
        Assert.Equal("servicebus", span.GetTagItem("messaging.system"));
        Assert.Equal("load-test", span.GetTagItem("messaging.destination.name"));
        Assert.Equal("message-1", span.GetTagItem("messaging.message.id"));
        Assert.Equal(3, span.GetTagItem("messaging.servicebus.message.delivery_count"));
        Assert.NotNull(span.GetTagItem("messaging.servicebus.message.enqueued_time"));
        Assert.Equal("completed", span.GetTagItem("dotqueue.message.outcome"));
        Assert.Equal(1, pump.Completed);
        var wait = Assert.Single(capture.For("dotqueue.message.wait"));
        Assert.InRange(wait.Value, 4.9, 15);
        Assert.Equal("s", wait.Unit);
        Assert.Equal("answers.assess.v1", wait.Tags["messageType"]);
        Assert.Single(capture.For("messaging.process.duration"));
        Assert.Equal("completed", Assert.Single(capture.For("dotqueue.message.settled")).Tags["outcome"]);
        Assert.Equal(0, capture.For("dotqueue.handler.active").Sum(x => x.Value));
        Assert.All(capture.Measurements, x => Assert.DoesNotContain("messaging.message.id", x.Tags.Keys));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RetriesArePerDeliveryAndLockRenewalsAreCounted(bool session)
    {
        using var capture = new TelemetryCapture();
        await using var pump = new TestPump(session, CreateMessage());
        var attempts = 0;
        await pump.StartAsync(async (_, _, renew, _) =>
        {
            attempts++;
            if (attempts < 3)
                throw new RetryableException("temporary");
            await renew();
        });
        await pump.DeliverAsync();

        Assert.Equal(3, attempts);
        var span = Assert.Single(capture.Spans);
        Assert.Equal(2, span.GetTagItem("dotqueue.retry.attempts"));
        Assert.NotEqual(ActivityStatusCode.Error, span.Status);
        Assert.Equal(2, capture.For("dotqueue.retry.attempts").Sum(x => x.Value));
        Assert.Equal(1, Assert.Single(capture.For("dotqueue.lock.renewals")).Value);
        Assert.Single(capture.For("messaging.process.duration"));
        Assert.Equal(1, pump.Renewed);
    }

    [Theory]
    [InlineData(false, "retryable", "abandoned")]
    [InlineData(true, "retryable", "abandoned")]
    [InlineData(false, "nonretryable", "dead-lettered")]
    [InlineData(true, "nonretryable", "dead-lettered")]
    [InlineData(false, "unexpected", "abandoned")]
    [InlineData(true, "unexpected", "abandoned")]
    public async Task HandlerFailuresRecordFinalSettlementAndError(bool session, string failure, string outcome)
    {
        using var capture = new TelemetryCapture();
        await using var pump = new TestPump(session, CreateMessage());
        await pump.StartAsync((_, _, _, _) => throw failure switch
        {
            "retryable" => new RetryableException("temporary"),
            "nonretryable" => new NonRetryableException("do not retry"),
            _ => new InvalidOperationException("unexpected"),
        });
        await pump.DeliverAsync();

        var span = Assert.Single(capture.Spans);
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal(outcome, span.GetTagItem("dotqueue.message.outcome"));
        Assert.Equal(outcome, Assert.Single(capture.For("dotqueue.message.settled")).Tags["outcome"]);
        Assert.Equal(0, capture.For("dotqueue.handler.active").Sum(x => x.Value));
        Assert.Equal(outcome == "abandoned" ? 1 : 0, pump.Abandoned);
        Assert.Equal(outcome == "dead-lettered" ? 1 : 0, pump.DeadLettered);
    }

    [Theory]
    [InlineData(false, "not json")]
    [InlineData(true, "not json")]
    [InlineData(false, "null")]
    [InlineData(true, "null")]
    public async Task InvalidPayloadIsDeadLetteredWithoutInvokingHandler(bool session, string body)
    {
        using var capture = new TelemetryCapture();
        await using var pump = new TestPump(session, CreateMessage(body));
        await pump.StartAsync((_, _, _, _) => throw new Xunit.Sdk.XunitException("Handler must not run"));
        await pump.DeliverAsync();
        Assert.Equal(1, pump.DeadLettered);
        Assert.Equal(ActivityStatusCode.Error, Assert.Single(capture.Spans).Status);
        Assert.Equal("dead-lettered", Assert.Single(capture.For("dotqueue.message.settled")).Tags["outcome"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SettlementLockLossIsNotReportedAsCompletedOrAbandoned(bool session)
    {
        using var capture = new TelemetryCapture();
        await using var pump = new TestPump(session, CreateMessage());
        pump.CompletionFailure = new ServiceBusException("lost", session
            ? ServiceBusFailureReason.SessionLockLost : ServiceBusFailureReason.MessageLockLost);
        await pump.StartAsync((_, _, _, _) => Task.CompletedTask);
        await pump.DeliverAsync();

        Assert.Equal("lock-expired", Assert.Single(capture.For("dotqueue.message.settled")).Tags["outcome"]);
        Assert.Equal(ActivityStatusCode.Error, Assert.Single(capture.Spans).Status);
        Assert.Equal(0, pump.Abandoned);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CapacityAndActiveHandlersDescribeSaturationAndDisposeRemovesCapacity(bool session)
    {
        using var capture = new TelemetryCapture();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var pump = new TestPump(session, CreateMessage());
        await pump.StartAsync(async (_, _, _, _) =>
        {
            entered.SetResult();
            await release.Task;
        });
        var delivery = pump.DeliverAsync();
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            capture.Meter.RecordObservableInstruments();
            Assert.Equal(3, Assert.Single(capture.For("dotqueue.handler.limit")).Value);
            Assert.Equal(1, capture.For("dotqueue.handler.active").Sum(x => x.Value));
        }
        finally
        {
            release.TrySetResult();
            await delivery;
        }
        capture.Clear();
        capture.Meter.RecordObservableInstruments();
        Assert.Equal(0, Assert.Single(capture.For("dotqueue.handler.active")).Value);
        await pump.DisposeAsync();
        capture.Clear();
        capture.Meter.RecordObservableInstruments();
        Assert.Empty(capture.For("dotqueue.handler.limit"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WithoutSubscribersHandlerAndAmbientActivityStillWork(bool session)
    {
        using var ambient = new Activity("caller").Start();
        await using var pump = new TestPump(session, CreateMessage());
        await pump.StartAsync((_, metadata, _, _) =>
        {
            Assert.Same(ambient, Activity.Current);
            Assert.NotNull(metadata);
            return Task.CompletedTask;
        });
        await pump.DeliverAsync();
        Assert.Same(ambient, Activity.Current);
        Assert.Equal(1, pump.Completed);
    }

    [Theory]
    [InlineData(false, "invalid")]
    [InlineData(true, "invalid")]
    [InlineData(false, null)]
    [InlineData(true, null)]
    public async Task MissingOrMalformedContextStartsRootAndRestoresAmbient(bool session, string? parent)
    {
        using var capture = new TelemetryCapture();
        using var ambient = new Activity("unrelated").Start();
        var properties = new Dictionary<string, object>();
        if (parent is not null) properties["traceparent"] = parent;
        await using var pump = new TestPump(session, CreateMessage(properties: properties));
        await pump.StartAsync((_, _, _, _) => Task.CompletedTask);
        await pump.DeliverAsync();

        var span = Assert.Single(capture.Spans);
        Assert.Equal(default, span.ParentSpanId);
        Assert.NotEqual(ambient.TraceId, span.TraceId);
        Assert.Same(ambient, Activity.Current);
        Assert.DoesNotContain("messageType", Assert.Single(capture.For("dotqueue.message.wait")).Tags.Keys);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedSettlementIsReportedOnceAndErrorPropagates(bool session)
    {
        using var capture = new TelemetryCapture();
        await using var pump = new TestPump(session, CreateMessage());
        var failure = new ServiceBusException("unavailable", ServiceBusFailureReason.ServiceCommunicationProblem);
        pump.CompletionFailure = failure;
        await pump.StartAsync((_, _, _, _) => Task.CompletedTask);
        Assert.Same(failure, await Assert.ThrowsAsync<ServiceBusException>(pump.DeliverAsync));

        Assert.Equal("settlement-failed", Assert.Single(capture.For("dotqueue.message.settled")).Tags["outcome"]);
        Assert.Equal(ActivityStatusCode.Error, Assert.Single(capture.Spans).Status);
        Assert.Equal(0, capture.For("dotqueue.handler.active").Sum(x => x.Value));
        Assert.Equal(0, pump.Abandoned);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LostLockDuringRenewalStopsSettlementAndIsNotCountedAsRenewed(bool session)
    {
        using var capture = new TelemetryCapture();
        await using var pump = new TestPump(session, CreateMessage());
        pump.RenewalFailure = new ServiceBusException("lost", session
            ? ServiceBusFailureReason.SessionLockLost : ServiceBusFailureReason.MessageLockLost);
        await pump.StartAsync((_, _, renew, _) => renew());
        await pump.DeliverAsync();

        Assert.Equal("lock-expired", Assert.Single(capture.For("dotqueue.message.settled")).Tags["outcome"]);
        Assert.Empty(capture.For("dotqueue.lock.renewals"));
        Assert.Equal(0, pump.Completed + pump.Abandoned + pump.DeadLettered);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ProcessorCancellationReachesHandlerAndAbandonmentIsRecorded(bool session)
    {
        using var capture = new TelemetryCapture();
        using var shutdown = new CancellationTokenSource();
        await using var pump = new TestPump(session, CreateMessage(), shutdown.Token);
        var invoked = false;
        await pump.StartAsync((_, _, _, token) =>
        {
            invoked = true;
            shutdown.Cancel();
            token.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        });
        await pump.DeliverAsync();

        Assert.True(invoked);
        Assert.Equal(1, pump.Abandoned);
        Assert.Equal("abandoned", Assert.Single(capture.For("dotqueue.message.settled")).Tags["outcome"]);
        Assert.Equal(ActivityStatusCode.Error, Assert.Single(capture.Spans).Status);
    }

    [Fact]
    public async Task CapacityAggregatesListenersForOneQueueAndFailedStartLeavesNoCapacity()
    {
        using var capture = new TelemetryCapture();
        await using var first = new TestPump(false, CreateMessage());
        await using var second = new TestPump(true, CreateMessage());
        await first.StartAsync((_, _, _, _) => Task.CompletedTask);
        await second.StartAsync((_, _, _, _) => Task.CompletedTask);
        capture.Meter.RecordObservableInstruments();
        Assert.Equal(6, Assert.Single(capture.For("dotqueue.handler.limit")).Value);
        await first.DisposeAsync();
        capture.Clear();
        capture.Meter.RecordObservableInstruments();
        Assert.Equal(3, Assert.Single(capture.For("dotqueue.handler.limit")).Value);
        await second.DisposeAsync();

        await using var failed = new TestPump(false, CreateMessage());
        failed.StartFailure = new InvalidOperationException("cannot start");
        await Assert.ThrowsAsync<InvalidOperationException>(() => failed.StartAsync((_, _, _, _) => Task.CompletedTask));
        capture.Clear();
        capture.Meter.RecordObservableInstruments();
        Assert.Empty(capture.For("dotqueue.handler.limit"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MetricsSubscriberJoiningDuringDeliveryObservesCurrentActiveCount(bool session)
    {
        using var traces = new TelemetryCapture(metrics: false);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var pump = new TestPump(session, CreateMessage());
        await pump.StartAsync(async (_, _, _, _) =>
        {
            entered.SetResult();
            await release.Task;
        });
        var delivery = pump.DeliverAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        using var metrics = new TelemetryCapture(traces: false);
        try
        {
            metrics.Meter.RecordObservableInstruments();
            Assert.Equal(1, Assert.Single(metrics.For("dotqueue.handler.active")).Value);
        }
        finally
        {
            release.TrySetResult();
            await delivery;
        }
        metrics.Clear();
        metrics.Meter.RecordObservableInstruments();
        Assert.Equal(0, Assert.Single(metrics.For("dotqueue.handler.active")).Value);
    }

    private static ServiceBusReceivedMessage CreateMessage(string body = "\"payload\"", IDictionary<string, object>? properties = null)
        => ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(body),
            messageId: "message-1",
            deliveryCount: 3,
            enqueuedTime: DateTimeOffset.UtcNow.AddSeconds(-5),
            lockedUntil: DateTimeOffset.UtcNow.AddMinutes(5),
            properties: properties);

    private sealed record Measurement(string Name, double Value, string? Unit, Dictionary<string, object?> Tags);

    private sealed class TelemetryCapture : IDisposable
    {
        public ConcurrentQueue<Activity> Spans { get; } = new();
        public ConcurrentQueue<Measurement> Measurements { get; } = new();
        public MeterListener Meter { get; } = new();
        private readonly ActivityListener _traces;

        public TelemetryCapture(bool metrics = true, bool traces = true)
        {
            _traces = new ActivityListener
            {
                ShouldListenTo = source => traces && source.Name == "DotQueue",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => Spans.Enqueue(activity),
            };
            ActivitySource.AddActivityListener(_traces);
            Meter.InstrumentPublished = (instrument, listener) =>
            {
                if (metrics && instrument.Meter.Name == "DotQueue")
                    listener.EnableMeasurementEvents(instrument);
            };
            Meter.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
            Meter.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
            Meter.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Record(instrument, value, tags));
            Meter.Start();
        }

        private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
            => Measurements.Enqueue(new(instrument.Name, value, instrument.Unit, tags.ToArray().ToDictionary(x => x.Key, x => x.Value)));

        public Measurement[] For(string name) => Measurements.Where(x => x.Name == name).ToArray();
        public void Clear() => Measurements.Clear();
        public void Dispose() { _traces.Dispose(); Meter.Dispose(); }
    }

    private sealed class PlainProcessor : ServiceBusProcessor
    {
        public Exception? StartFailure { get; set; }
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default)
            => StartFailure is null ? Task.CompletedTask : Task.FromException(StartFailure);
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task DeliverAsync(ProcessMessageEventArgs args) => OnProcessMessageAsync(args);
    }

    private sealed class SessionProcessor : ServiceBusSessionProcessor
    {
        protected override ServiceBusProcessor InnerProcessor { get; } = new PlainProcessor();
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task DeliverAsync(ProcessSessionMessageEventArgs args) => OnProcessSessionMessageAsync(args);
    }

    private sealed class TestPump : IAsyncDisposable
    {
        private readonly PlainProcessor _plain = new();
        private readonly SessionProcessor _session = new();
        private readonly bool _isSession;
        private readonly Mock<ServiceBusReceiver> _receiver = new();
        private readonly Mock<ServiceBusSessionReceiver> _sessionReceiver = new();
        private readonly ServiceBusReceivedMessage _message;
        private readonly CancellationToken _processorCancellation;
        private readonly IQueueListener<string> _listener;
        public int Completed { get; private set; }
        public int Abandoned { get; private set; }
        public int DeadLettered { get; private set; }
        public int Renewed { get; private set; }
        public Exception? CompletionFailure { get; set; }
        public Exception? RenewalFailure { get; set; }
        public Exception? StartFailure { set => _plain.StartFailure = value; }

        public TestPump(bool session, ServiceBusReceivedMessage message, CancellationToken processorCancellation = default)
        {
            _isSession = session;
            _message = message;
            _processorCancellation = processorCancellation;
            var client = new Mock<ServiceBusClient>();
            client.Setup(x => x.CreateProcessor("load-test", It.IsAny<ServiceBusProcessorOptions>())).Returns(_plain);
            client.Setup(x => x.CreateSessionProcessor("load-test", It.IsAny<ServiceBusSessionProcessorOptions>())).Returns(_session);
            var settings = new QueueSettings { MaxConcurrentCalls = 3, MaxRetryAttempts = 2, RetryDelaySeconds = 0 };
            _listener = session
                ? new AzureServiceBusSessionQueueListener<string>(client.Object, "load-test", settings, new RetryPolicyProvider(), NullLogger<AzureServiceBusSessionQueueListener<string>>.Instance)
                : new AzureServiceBusQueueListener<string>(client.Object, "load-test", settings, new RetryPolicyProvider(), NullLogger<AzureServiceBusQueueListener<string>>.Instance);
            ConfigureReceiver(_receiver);
            ConfigureReceiver(_sessionReceiver);
            _sessionReceiver.SetupGet(x => x.SessionLockedUntil).Returns(DateTimeOffset.UtcNow.AddMinutes(5));
            _sessionReceiver.Setup(x => x.RenewSessionLockAsync(It.IsAny<CancellationToken>())).Returns(RenewAsync);
        }

        private void ConfigureReceiver<TReceiver>(Mock<TReceiver> receiver) where TReceiver : ServiceBusReceiver
        {
            receiver.Setup(x => x.CompleteMessageAsync(_message, It.IsAny<CancellationToken>())).Returns(() =>
            {
                if (CompletionFailure is not null) return Task.FromException(CompletionFailure);
                Completed++;
                return Task.CompletedTask;
            });
            receiver.Setup(x => x.AbandonMessageAsync(_message, It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>())).Callback(() => Abandoned++).Returns(Task.CompletedTask);
            receiver.Setup(x => x.DeadLetterMessageAsync(_message, It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>())).Callback(() => DeadLettered++).Returns(Task.CompletedTask);
            receiver.Setup(x => x.RenewMessageLockAsync(_message, It.IsAny<CancellationToken>())).Returns(RenewAsync);
        }

        private Task RenewAsync()
        {
            if (RenewalFailure is not null) return Task.FromException(RenewalFailure);
            Renewed++;
            return Task.CompletedTask;
        }

        public Task StartAsync(Func<string, IReadOnlyDictionary<string, string>?, Func<Task>, CancellationToken, Task> handler)
            => _listener.StartAsync(handler, CancellationToken.None);
        public Task DeliverAsync() => _isSession
            ? _session.DeliverAsync(new ProcessSessionMessageEventArgs(_message, _sessionReceiver.Object, "processor", _processorCancellation))
            : _plain.DeliverAsync(new ProcessMessageEventArgs(_message, _receiver.Object, "processor", _processorCancellation));
        public ValueTask DisposeAsync() => ((IAsyncDisposable)_listener).DisposeAsync();
    }
}
