using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Azure.Messaging.ServiceBus;

namespace DotQueue.Azure;

internal sealed class AzureQueueTelemetry(string queueName, int handlerLimit) : IDisposable
{
    private static readonly ActivitySource Source = new("DotQueue");
    private static readonly Meter Meter = new("DotQueue");
    private static readonly Histogram<double> Wait = Meter.CreateHistogram<double>("dotqueue.message.wait", "s");
    private static readonly Histogram<double> Processing = Meter.CreateHistogram<double>("messaging.process.duration", "s");
    private static readonly Counter<long> Settled = Meter.CreateCounter<long>("dotqueue.message.settled", "{message}");
    private static readonly Counter<long> Retries = Meter.CreateCounter<long>("dotqueue.retry.attempts", "{retry}");
    private static readonly Counter<long> Renewals = Meter.CreateCounter<long>("dotqueue.lock.renewals", "{renewal}");
    private static readonly ConcurrentDictionary<AzureQueueTelemetry, byte> Listeners = new();
    private static readonly ObservableGauge<long> Limit = Meter.CreateObservableGauge("dotqueue.handler.limit", ObserveLimits, "{handler}");
    private static readonly ObservableUpDownCounter<long> Active = Meter.CreateObservableUpDownCounter("dotqueue.handler.active", ObserveActive, "{handler}");

    private readonly string _queueName = queueName;
    private readonly int _handlerLimit = handlerLimit;
    private long _active;

    public void Start() => Listeners.TryAdd(this, 0);

    public void Dispose() => Listeners.TryRemove(this, out _);

    private static IEnumerable<Measurement<long>> ObserveLimits()
        => Listeners.Keys.GroupBy(x => x._queueName).Select(group => new Measurement<long>(
            group.Sum(x => (long)x._handlerLimit),
            new KeyValuePair<string, object?>("messaging.destination.name", group.Key)));

    private static IEnumerable<Measurement<long>> ObserveActive()
        => Listeners.Keys.GroupBy(x => x._queueName).Select(group => new Measurement<long>(
            group.Sum(x => Volatile.Read(ref x._active)),
            new KeyValuePair<string, object?>("messaging.destination.name", group.Key)));

    public DeliveryScope StartDelivery(ServiceBusReceivedMessage message)
    {
        Interlocked.Increment(ref _active);
        if (!Source.HasListeners() && !Wait.Enabled && !Processing.Enabled &&
            !Settled.Enabled && !Retries.Enabled && !Renewals.Enabled)
            return new DeliveryScope(this, null);

        try
        {
            return new DeliveryScope(this, new Delivery(_queueName, message));
        }
        catch
        {
            Interlocked.Decrement(ref _active);
            throw;
        }
    }

    internal readonly struct DeliveryScope(AzureQueueTelemetry owner, Delivery? telemetry) : IDisposable
    {
        public Delivery? Telemetry { get; } = telemetry;

        public void Dispose()
        {
            try
            {
                Telemetry?.Dispose();
            }
            finally
            {
                Interlocked.Decrement(ref owner._active);
            }
        }
    }

    internal sealed class Delivery : IDisposable
    {
        private readonly TagList _tags;
        private readonly Activity? _activity;
        private readonly Activity? _previousActivity;
        private readonly ServiceBusReceivedMessage _message;
        private int _attempts;
        private long _handlerStarted;
        private double _processingSeconds;
        private bool _disposed;
        private bool _settled;

        public Delivery(string queueName, ServiceBusReceivedMessage message)
        {
            _message = message;
            _tags = new TagList { { "messaging.destination.name", queueName } };
            if (message.ApplicationProperties.TryGetValue("messageType", out var messageType) && messageType is string type && !string.IsNullOrEmpty(type))
                _tags.Add("messageType", type);

            if (Source.HasListeners())
            {
                message.ApplicationProperties.TryGetValue("traceparent", out var traceParent);
                message.ApplicationProperties.TryGetValue("tracestate", out var traceState);
                ActivityContext.TryParse(traceParent as string, traceState as string, isRemote: true, out var parent);
                _previousActivity = Activity.Current;
                Activity.Current = null;
                try
                {
                    _activity = Source.StartActivity($"{queueName} process", ActivityKind.Consumer, parent);
                }
                finally
                {
                    Activity.Current = _activity ?? _previousActivity;
                }
                _activity?.SetTag("messaging.system", "servicebus");
                _activity?.SetTag("messaging.destination.name", queueName);
                _activity?.SetTag("messaging.operation.name", "process");
                _activity?.SetTag("messaging.operation.type", "process");
                _activity?.SetTag("messaging.message.id", message.MessageId);
                _activity?.SetTag("messaging.servicebus.message.delivery_count", message.DeliveryCount);
                _activity?.SetTag("messaging.servicebus.message.enqueued_time", message.EnqueuedTime.ToString("O"));
                if (messageType is string typeName)
                    _activity?.SetTag("messageType", typeName);
            }
        }

        public void HandlerStarted()
        {
            if (_attempts++ == 0)
            {
                var seconds = Math.Max(0, (DateTimeOffset.UtcNow - _message.EnqueuedTime).TotalSeconds);
                Wait.Record(seconds, _tags);
                _activity?.SetTag("dotqueue.message.wait", seconds);
            }
            else
            {
                Retries.Add(1, _tags);
                _activity?.SetTag("dotqueue.retry.attempts", _attempts - 1);
            }
            _handlerStarted = Stopwatch.GetTimestamp();
        }

        public void HandlerStopped() => _processingSeconds += Stopwatch.GetElapsedTime(_handlerStarted).TotalSeconds;

        public void LockRenewed() => Renewals.Add(1, _tags);

        public void Fail(Exception exception)
        {
            _activity?.SetStatus(ActivityStatusCode.Error);
            _activity?.SetTag("error.type", exception.GetType().FullName);
        }

        public void Settle(string outcome)
        {
            if (_settled)
                return;
            _settled = true;
            var tags = _tags;
            tags.Add("outcome", outcome);
            Settled.Add(1, tags);
            _activity?.SetTag("dotqueue.message.outcome", outcome);
            if (outcome != "completed")
                _activity?.SetStatus(ActivityStatusCode.Error);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            if (_attempts > 0)
                Processing.Record(_processingSeconds, _tags);
            _activity?.SetTag("dotqueue.retry.attempts", Math.Max(0, _attempts - 1));
            _activity?.Dispose();
            if (_activity is not null)
                Activity.Current = _previousActivity;
        }
    }
}
