using Microsoft.Extensions.Logging;
using Polly;

namespace DotQueue;

public interface IRetryPolicyProvider
{
    IAsyncPolicy Create(QueueSettings settings, ILogger logger);
}

public class RetryPolicyProvider : IRetryPolicyProvider
{
    public IAsyncPolicy Create(QueueSettings settings, ILogger logger)
    {
        return Policy
            .Handle<Exception>(ShouldRetry)
            .WaitAndRetryAsync(
                retryCount: settings.MaxRetryAttempts,
                sleepDurationProvider: attempt => TimeSpan.FromSeconds(settings.RetryDelaySeconds),
                onRetry: (exception, delay, retryAttempt, _) =>
                {
                    logger.LogWarning(exception, "Retry {RetryAttempt} in {Delay}", retryAttempt, delay);
                });
    }
    private bool ShouldRetry(Exception ex)
    {
        return ex switch
        {
            RetryableException => true,
            NonRetryableException => false,
            _ => false
        };
    }
}
