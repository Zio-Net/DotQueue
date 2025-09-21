using DotQueue.Rabbit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace DotQueue.Rabbit;

public static class RabbitMqServiceCollectionExtensions
{
    public static IServiceCollection AddRabbitMQQueue<T, THandler>(
        this IServiceCollection services,
        string amqpConnectionString,
        string queueName,
        Action<QueueSettings>? configure = null)
        where THandler : class, IQueueHandler<T>
    {
        services.AddScoped<IQueueHandler<T>, THandler>();

        var settings = new QueueSettings();
        configure?.Invoke(settings);
        services.AddSingleton(settings);

        services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();

        services.AddSingleton<IConnectionFactory>(_ => new ConnectionFactory
        {
            Uri = new Uri(amqpConnectionString),
        });

        services.AddSingleton<IQueueListener<T>>(sp =>
            new RabbitMqQueueListener<T>(
                sp.GetRequiredService<IConnectionFactory>(),
                queueName,
                sp.GetRequiredService<QueueSettings>(),
                sp.GetRequiredService<IRetryPolicyProvider>(),
                sp.GetRequiredService<ILogger<RabbitMqQueueListener<T>>>()
            ));

        services.AddHostedService<QueueProcessor<T>>();
        return services;
    }
}
