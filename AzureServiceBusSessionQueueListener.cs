using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace DotQueue;

public class AzureServiceBusSessionQueueListener<T> : IQueueListener<T>, IAsyncDisposable
{
    private readonly ServiceBusSessionProcessor _processor;
    private readonly IRetryPolicyProvider _retryPolicyProvider;
    private readonly QueueSettings _settings;
    private readonly ILogger<AzureServiceBusSessionQueueListener<T>> _logger;

    public AzureServiceBusSessionQueueListener(
        ServiceBusClient client,
        string queueName,
        QueueSettings settings,
        IRetryPolicyProvider retryPolicyProvider,
        ILogger<AzureServiceBusSessionQueueListener<T>> logger)
    {
        _settings = settings;
        _retryPolicyProvider = retryPolicyProvider;
        _logger = logger;

        _processor = client.CreateSessionProcessor(queueName, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = settings.MaxConcurrentCalls,
            PrefetchCount = settings.PrefetchCount,
            AutoCompleteMessages = false
        });
    }

    public async Task StartAsync(Func<T, IReadOnlyDictionary<string, string>?, Func<Task>, CancellationToken, Task> handler, CancellationToken cancellationToken)
    {
        var retryPolicy = _retryPolicyProvider.Create(_settings, _logger);

        _processor.ProcessMessageAsync += async args =>
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            void ArmCancelAfterFromLockedUntil()
            {
                var now = DateTimeOffset.UtcNow;
                var timeout = args.SessionLockedUntil - now - TimeSpan.FromSeconds(2);
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
                await args.RenewSessionLockAsync(linkedCts.Token);
                _logger.LogDebug("Lock renewed for session {SessionId}", args.SessionId);
                _logger.LogDebug("Session lock till: {Time}", args.SessionLockedUntil);
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

                var metadata = args.Message.ApplicationProperties
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value?.ToString() ?? string.Empty);

                await retryPolicy.ExecuteAsync(async () =>
                {
                    await handler(msg, metadata, renewLock, linkedCts.Token);

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
            catch (JsonException jex)
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
            if (args.Exception is ServiceBusException sbex)
            {
                if (sbex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
                {
                    _logger.LogWarning("Entity not found while processing.");
                }
                else if (sbex.Reason == ServiceBusFailureReason.ServiceCommunicationProblem)
                {
                    _logger.LogWarning("Service communication problem - emulator not ready.");
                }
                else
                {
                    _logger.LogWarning("Service Bus error: {Reason}", sbex.Reason);
                }
            }
            else
            {
                _logger.LogError(args.Exception, "Message handler error");
            }

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
