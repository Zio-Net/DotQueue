using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace DotQueue;

public class AzureServiceBusQueueListener<T> : IQueueListener<T>, IAsyncDisposable
{
    private readonly ServiceBusProcessor _processor;
    private readonly IRetryPolicyProvider _retryPolicyProvider;
    private readonly QueueSettings _settings;
    private readonly ILogger<AzureServiceBusQueueListener<T>> _logger;

    public AzureServiceBusQueueListener(
        ServiceBusClient client,
        string queueName,
        QueueSettings settings,
        IRetryPolicyProvider retryPolicyProvider,
        ILogger<AzureServiceBusQueueListener<T>> logger)
    {
        _settings = settings;
        _retryPolicyProvider = retryPolicyProvider;
        _logger = logger;

        _processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = settings.MaxConcurrentCalls,
            PrefetchCount = settings.PrefetchCount,
            AutoCompleteMessages = false
        });
    }

    public async Task StartAsync(Func<T, Func<Task>, CancellationToken, Task> handler, CancellationToken cancellationToken)
    {
        var retryPolicy = _retryPolicyProvider.Create(_settings, _logger);

        _processor.ProcessMessageAsync += async args =>
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            void ArmCancelAfterFromLockedUntil()
            {
                var now = DateTimeOffset.UtcNow;
                var timeout = args.Message.LockedUntil - now - TimeSpan.FromSeconds(2);
                if (timeout <= TimeSpan.Zero)
                {
                    linkedCts.Cancel();
                }
                else
                {
                    linkedCts.CancelAfter(timeout);
                }
            }

            ArmCancelAfterFromLockedUntil();

            var renewLock = async () =>
            {
                await args.RenewMessageLockAsync(args.Message, linkedCts.Token);
                _logger.LogDebug("Lock renewed for message {MessageId}", args.Message.MessageId);
                _logger.LogDebug("Lock till: {Time}", args.Message.LockedUntil);

                ArmCancelAfterFromLockedUntil();
            };

            try
            {
                var json = args.Message.Body.ToString();
                _logger.LogDebug("Raw message body: {Json}", json);
                var jsonOptions = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true,
                };
                var msg = JsonSerializer.Deserialize<T>(json, jsonOptions);

                if (msg == null)
                {
                    _logger.LogWarning("Failed to deserialize message.");
                    await args.DeadLetterMessageAsync(args.Message, cancellationToken: cancellationToken);
                    return;
                }

                await retryPolicy.ExecuteAsync(async () =>
                {
                    await handler(msg, renewLock, linkedCts.Token);

                    if (_settings.ProcessingDelayMs > 0)
                    {
                        await Task.Delay(_settings.ProcessingDelayMs, linkedCts.Token);
                    }
                });

                await args.CompleteMessageAsync(args.Message, cancellationToken);
            }
            catch (RetryableException rex)
            {
                _logger.LogWarning(rex, "Retryable error. Abandoning message.");
                await args.AbandonMessageAsync(args.Message, cancellationToken: cancellationToken);
            }
            catch (NonRetryableException nex)
            {
                _logger.LogWarning(nex, "Non-retryable error. Dead-lettering message.");
                await args.DeadLetterMessageAsync(args.Message, cancellationToken: cancellationToken);
            }
            catch (OperationCanceledException) when (linkedCts.IsCancellationRequested)
            {
                _logger.LogWarning("Handler exceeded lock duration. Abandoning message.");
                await args.AbandonMessageAsync(args.Message, cancellationToken: cancellationToken);
            }
            catch(JsonException jex)
            {
                _logger.LogWarning(jex, "Error while deserializing message.");
                await args.DeadLetterMessageAsync(args.Message, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled error. Treating as retryable.");
                await args.AbandonMessageAsync(args.Message, cancellationToken: cancellationToken);
            }
        };

        _processor.ProcessErrorAsync += args =>
        {
            _logger.LogError(args.Exception, "Message handler error");
            return Task.CompletedTask;
        };

        await _processor.StartProcessingAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _processor.DisposeAsync();
        GC.SuppressFinalize(this);
    }
}

