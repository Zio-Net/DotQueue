using DotQueue;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

public sealed class RabbitMqQueueListener<T> : IQueueListener<T>, IAsyncDisposable
{
    private readonly IConnectionFactory _factory;
    private IConnection? _conn;
    private IChannel? _ch;
    private readonly string _queue;
    private readonly QueueSettings _settings;
    private readonly IRetryPolicyProvider _retry;
    private readonly ILogger<RabbitMqQueueListener<T>> _log;
    private readonly JsonSerializerOptions _json = new() { PropertyNameCaseInsensitive = true };

    public RabbitMqQueueListener(
        IConnectionFactory factory,
        string queueName,
        QueueSettings settings,
        IRetryPolicyProvider retryPolicyProvider,
        ILogger<RabbitMqQueueListener<T>> log)
    {
        _factory = factory;
        _queue = queueName;
        _settings = settings;
        _retry = retryPolicyProvider;
        _log = log;
    }

    public async Task StartAsync(
        Func<T, IReadOnlyDictionary<string, string>?, Func<Task>, CancellationToken, Task> handler,
        CancellationToken ct)
    {
        _conn = await _factory.CreateConnectionAsync(ct).ConfigureAwait(false);
        _ch = await _conn.CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);

        if (_settings.PrefetchCount > 0)
            await _ch.BasicQosAsync(0, (ushort)_settings.PrefetchCount, false, ct).ConfigureAwait(false);

        var consumer = new AsyncEventingBasicConsumer(_ch);
        var gate = new SemaphoreSlim(Math.Max(1, _settings.MaxConcurrentCalls));

        consumer.ReceivedAsync += async (_, ea) =>
        {
            await gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var bodyBytes = ea.Body.ToArray();

                var msg = JsonSerializer.Deserialize<T>(bodyBytes, _json)
                          ?? throw new NonRetryableException("Deserialize failed");

                var meta = ea.BasicProperties?.Headers?
                    .ToDictionary(
                        kvp => kvp.Key,
                        kvp => kvp.Value switch
                        {
                            null => string.Empty,
                            byte[] b => Encoding.UTF8.GetString(b),
                            ReadOnlyMemory<byte> rom => Encoding.UTF8.GetString(rom.Span),
                            _ => kvp.Value.ToString() ?? string.Empty
                        });

                var policy = _retry.Create(_settings, _log);
                await policy.ExecuteAsync(async () =>
                {
                    await handler(msg, meta, () => Task.CompletedTask, ct).ConfigureAwait(false);
                    await _ch!.BasicAckAsync(ea.DeliveryTag, multiple: false, ct).ConfigureAwait(false);
                }).ConfigureAwait(false);
            }
            catch (RetryableException ex)
            {
                _log.LogWarning(ex, "Retryable, requeue");
                await _ch!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Dead-letter");
                await _ch!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, ct).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
        };

        await _ch.BasicConsumeAsync(
            queue: _queue,
            autoAck: false,
            consumer: consumer,
            cancellationToken: ct
        ).ConfigureAwait(false);
    }
    public async ValueTask DisposeAsync()
    {
        try { if (_ch is not null) await _ch.DisposeAsync().ConfigureAwait(false); } catch { }
        try { if (_conn is not null) await _conn.DisposeAsync().ConfigureAwait(false); } catch { }
    }
}
