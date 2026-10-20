using System.Text.Json;
using BeanNotifs.HealthChecks;
using BeanNotifs.Messaging;
using BeanNotifs.Models;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection(RabbitMqOptions.SectionName));
builder.Services.AddSingleton<NotificationStore>();

builder.Services.AddSingleton<IReadOnlyList<ConsumerBinding>>(sp =>
{
    var store = sp.GetRequiredService<NotificationStore>();
    return new List<ConsumerBinding>
    {
        new("notifs.payment-failed", "payment.failed", envelope => HandlePaymentFailedAsync(envelope, store)),
        new("notifs.order-cooked", "order.cooked", envelope => HandleOrderCookedAsync(envelope, store)),
    };
});

// Registered both as its own type (so the health check and DI can resolve it directly)
// and as the IHostedService that actually gets started — same singleton instance either way.
builder.Services.AddSingleton<EventConsumerHostedService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<EventConsumerHostedService>());

builder.Services.AddHealthChecks()
    .AddCheck("self", () => Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy(), tags: ["live"])
    .AddCheck<RabbitMqHealthCheck>("rabbitmq", tags: ["ready"]);

var app = builder.Build();

app.UseHttpMetrics();
app.MapMetrics();

app.MapHealthChecks("/healthz/live", new() { Predicate = c => c.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new() { Predicate = c => c.Tags.Contains("ready") });

app.MapGet("/notifications", (NotificationStore store) => store.GetAll());

app.Run();

static Task HandlePaymentFailedAsync(EventEnvelope envelope, NotificationStore store)
{
    var payload = envelope.Payload.Deserialize<PaymentFailedPayload>()!;
    store.Add(new Notification(envelope.OrderId, "payment-failed", $"Payment failed: {payload.Reason}", envelope.OccurredAt));
    return Task.CompletedTask;
}

static Task HandleOrderCookedAsync(EventEnvelope envelope, NotificationStore store)
{
    store.Add(new Notification(envelope.OrderId, "order-cooked", "Your order is ready!", envelope.OccurredAt));
    return Task.CompletedTask;
}
