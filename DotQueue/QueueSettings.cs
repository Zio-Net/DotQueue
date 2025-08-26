namespace DotQueue;

public class QueueSettings
{
    public int MaxConcurrentCalls { get; set; } = 1;
    public int PrefetchCount { get; set; } = 0;
    public int ProcessingDelayMs { get; set; } = 0;
    public int MaxRetryAttempts { get; set; } = 3;
    public int RetryDelaySeconds { get; set; } = 2;
}
