using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DotQueue;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddQueue<T, THandler>(
        this IServiceCollection services,
        string queueName,
        Action<QueueSettings>? configure = null)
        where THandler : class, IQueueHandler<T>
    {
        services.AddScoped<IQueueHandler<T>, THandler>();

        var settings = new QueueSettings();
        configure?.Invoke(settings);
        services.AddSingleton(settings);
        services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();

        services.AddSingleton<IQueueListener<T>>(sp =>
            new AzureServiceBusQueueListener<T>(
                sp.GetRequiredService<ServiceBusClient>(),
                queueName,
                settings,
                sp.GetRequiredService<IRetryPolicyProvider>(),
                sp.GetRequiredService<ILogger<AzureServiceBusQueueListener<T>>>()));

        services.AddHostedService<QueueProcessor<T>>();

        return services;
    }
}
