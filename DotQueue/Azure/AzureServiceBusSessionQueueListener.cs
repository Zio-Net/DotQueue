using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace DotQueue.Azure;

public class AzureServiceBusSessionQueueListener<T> : IQueueListener<T>, IAsyncDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly ServiceBusSessionProcessor _processor;
    private readonly IRetryPolicyProvider _retryPolicyProvider;
    private readonly QueueSettings _settings;
    private readonly ILogger<AzureServiceBusSessionQueueListener<T>> _logger;
    private readonly AzureQueueTelemetry _telemetry;

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
        _telemetry = new AzureQueueTelemetry(queueName, settings.MaxConcurrentCalls);
    }

    public async Task StartAsync(Func<T, IReadOnlyDictionary<string, string>?, Func<Task>, CancellationToken, Task> handler, CancellationToken cancellationToken)
    {
        var retryPolicy = _retryPolicyProvider.Create(_settings, _logger);

        _processor.ProcessMessageAsync += async args =>
        {
            using var deliveryScope = _telemetry.StartDelivery(args.Message);
            var delivery = deliveryScope.Telemetry;
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, args.CancellationToken);
            var outcome = "completed";

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
                delivery?.LockRenewed();
                _logger.LogDebug("Lock renewed for session {SessionId}", args.SessionId);
                _logger.LogDebug("Lock till: {Time}", args.SessionLockedUntil);

                ArmCancelAfterFromLockedUntil();
            };

            try
            {
                var json = args.Message.Body.ToString();
                _logger.LogDebug("Raw message body: {Json}", json);
                var msg = DeserializeMessage(json);

                if (msg == null)
                {
                    _logger.LogWarning("Failed to deserialize message.");
                    outcome = "dead-lettered";
                }
                else
                {
                    var metadata = args.Message.ApplicationProperties
                        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value?.ToString() ?? string.Empty);
                    await retryPolicy.ExecuteAsync(async () =>
                    {
                        delivery?.HandlerStarted();
                        try
                        {
                            await handler(msg, metadata, renewLock, linkedCts.Token);
                            if (_settings.ProcessingDelayMs > 0)
                                await Task.Delay(_settings.ProcessingDelayMs, linkedCts.Token);
                        }
                        finally
                        {
                            delivery?.HandlerStopped();
                        }
                    });
                }
            }
            catch (RetryableException rex)
            {
                _logger.LogWarning(rex, "Retryable error. Abandoning message.");
                delivery?.Fail(rex);
                outcome = "abandoned";
            }
            catch (NonRetryableException nex)
            {
                _logger.LogWarning(nex, "Non-retryable error. Dead-lettering message.");
                delivery?.Fail(nex);
                outcome = "dead-lettered";
            }
            catch (ServiceBusException ex) when (ex.Reason is ServiceBusFailureReason.MessageLockLost or ServiceBusFailureReason.SessionLockLost)
            {
                _logger.LogWarning(ex, "Message lock lost during processing.");
                delivery?.Fail(ex);
                delivery?.Settle("lock-expired");
                return;
            }
            catch (OperationCanceledException ex) when (linkedCts.IsCancellationRequested)
            {
                _logger.LogWarning(ex, "Handler cancelled. Abandoning message.");
                delivery?.Fail(ex);
                outcome = "abandoned";
            }
            catch(JsonException jex)
            {
                _logger.LogWarning(jex, "Error while deserializing message.");
                delivery?.Fail(jex);
                outcome = "dead-lettered";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled error. Treating as retryable.");
                delivery?.Fail(ex);
                outcome = "abandoned";
            }

            try
            {
                switch (outcome)
                {
                    case "completed":
                        await args.CompleteMessageAsync(args.Message, cancellationToken);
                        break;
                    case "dead-lettered":
                        await args.DeadLetterMessageAsync(args.Message, cancellationToken: cancellationToken);
                        break;
                    default:
                        await args.AbandonMessageAsync(args.Message, cancellationToken: cancellationToken);
                        break;
                }
                delivery?.Settle(outcome);
            }
            catch (ServiceBusException ex) when (ex.Reason is ServiceBusFailureReason.MessageLockLost or ServiceBusFailureReason.SessionLockLost)
            {
                _logger.LogWarning(ex, "Message lock lost during settlement.");
                delivery?.Fail(ex);
                delivery?.Settle("lock-expired");
            }
            catch (Exception ex)
            {
                delivery?.Fail(ex);
                delivery?.Settle("settlement-failed");
                throw;
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
        _telemetry.Start();
        try
        {
            await _processor.StartProcessingAsync(cancellationToken);
        }
        catch
        {
            _telemetry.Dispose();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _processor.DisposeAsync();
        }
        finally
        {
            _telemetry.Dispose();
            GC.SuppressFinalize(this);
        }
    }

    private static T? DeserializeMessage(string json)
    {
        if (typeof(T) == typeof(RawQueueMessage))
        {
            return (T)(object)new RawQueueMessage(json);
        }

        return JsonSerializer.Deserialize<T>(json, JsonOptions);
    }
}

