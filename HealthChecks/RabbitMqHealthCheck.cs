using Microsoft.Extensions.Diagnostics.HealthChecks;
using BeanNotifs.Messaging;

namespace BeanNotifs.HealthChecks;

// BeanNotifs has no EventPublisher (consumer-only, per Q1), so this probes the
// consumer hosted service's own connection instead.
public class RabbitMqHealthCheck(EventConsumerHostedService consumer) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var result = consumer.IsConnected
            ? HealthCheckResult.Healthy("RabbitMQ connection open")
            : HealthCheckResult.Unhealthy("RabbitMQ connection closed");

        return Task.FromResult(result);
    }
}
