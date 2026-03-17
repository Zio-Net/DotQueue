using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DotQueue.Azure;

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

    public static IServiceCollection AddSessionQueue<T, THandler>(
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
            new AzureServiceBusSessionQueueListener<T>(
                sp.GetRequiredService<ServiceBusClient>(),
                queueName,
                settings,
                sp.GetRequiredService<IRetryPolicyProvider>(),
                sp.GetRequiredService<ILogger<AzureServiceBusSessionQueueListener<T>>>()));

        services.AddHostedService<QueueProcessor<T>>();

        return services;
    }

    public static IServiceCollection AddTypedRoutedQueue<TRoutedHandler>(
        this IServiceCollection services,
        string queueName,
        Action<QueueSettings>? configure = null)
        where TRoutedHandler : TypedRoutedQueueHandler
    {
        services.AddScoped<TRoutedHandler>();
        services.AddScoped<IQueueHandler<RawQueueMessage>>(sp => sp.GetRequiredService<TRoutedHandler>());

        var settings = new QueueSettings();
        configure?.Invoke(settings);
        services.AddSingleton(settings);
        services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();

        services.AddSingleton<IQueueListener<RawQueueMessage>>(sp =>
            new AzureServiceBusQueueListener<RawQueueMessage>(
                sp.GetRequiredService<ServiceBusClient>(),
                queueName,
                settings,
                sp.GetRequiredService<IRetryPolicyProvider>(),
                sp.GetRequiredService<ILogger<AzureServiceBusQueueListener<RawQueueMessage>>>()));

        services.AddHostedService<QueueProcessor<RawQueueMessage>>();

        return services;
    }

    public static IServiceCollection AddTypedRoutedSessionQueue<TRoutedHandler>(
        this IServiceCollection services,
        string queueName,
        Action<QueueSettings>? configure = null)
        where TRoutedHandler : TypedRoutedQueueHandler
    {
        services.AddScoped<TRoutedHandler>();
        services.AddScoped<IQueueHandler<RawQueueMessage>>(sp => sp.GetRequiredService<TRoutedHandler>());

        var settings = new QueueSettings();
        configure?.Invoke(settings);
        services.AddSingleton(settings);
        services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();

        services.AddSingleton<IQueueListener<RawQueueMessage>>(sp =>
            new AzureServiceBusSessionQueueListener<RawQueueMessage>(
                sp.GetRequiredService<ServiceBusClient>(),
                queueName,
                settings,
                sp.GetRequiredService<IRetryPolicyProvider>(),
                sp.GetRequiredService<ILogger<AzureServiceBusSessionQueueListener<RawQueueMessage>>>()));

        services.AddHostedService<QueueProcessor<RawQueueMessage>>();

        return services;
    }
}
