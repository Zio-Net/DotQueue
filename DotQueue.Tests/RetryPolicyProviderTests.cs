using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DotQueue.Tests;

public class RetryPolicyProviderTests
{
    [Fact]
    public async Task RetriesRetryableException()
    {
        var settings = new QueueSettings
        {
            MaxRetryAttempts = 3,
            RetryDelaySeconds = 0 
        };
        var logger = Mock.Of<ILogger>();
        var provider = new RetryPolicyProvider();
        var policy = provider.Create(settings, logger);

        int attempts = 0;
        Func<Task> action = async () =>
        {
            attempts++;
            if (attempts < 4) 
            {
                throw new RetryableException("retry me");
            }
            await Task.CompletedTask;
        };

        await policy.ExecuteAsync(action);

        attempts.Should().Be(4, "it should do original try plus 3 retries for RetryableException");
    }

    [Fact]
    public async Task DoesNotRetryNonRetryableException()
    {
        var settings = new QueueSettings
        {
            MaxRetryAttempts = 5,
            RetryDelaySeconds = 0
        };
        var logger = Mock.Of<ILogger>();
        var provider = new RetryPolicyProvider();
        var policy = provider.Create(settings, logger);

        int attempts = 0;
        Func<Task> action = () =>
        {
            attempts++;
            throw new NonRetryableException("do not retry");
        };

        var ex = await Assert.ThrowsAsync<NonRetryableException>(async () => await policy.ExecuteAsync(action));
        ex.Message.Should().Contain("do not retry");
        attempts.Should().Be(1, "non-retryable should not be retried");
    }
    [Fact]
    public async Task ExecutesWithoutRetryOnSuccess()
    {
        var settings = new QueueSettings { MaxRetryAttempts = 3, RetryDelaySeconds = 0 };
        var logger = Mock.Of<ILogger>();
        var provider = new RetryPolicyProvider();
        var policy = provider.Create(settings, logger);
        int attempts = 0;
        await policy.ExecuteAsync(() =>
        {
            attempts++;
            return Task.CompletedTask;
        });
        attempts.Should().Be(1, "successful execution should not retry");
    }
}
