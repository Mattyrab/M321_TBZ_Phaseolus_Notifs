using System.Collections.Concurrent;

namespace BeanNotifs.Models;

// payment.failed payload, as published by PayBeancoins
public record PaymentFailedPayload(string Reason);

public record Notification(Guid OrderId, string Type, string Message, DateTimeOffset OccurredAt);

// In-memory only — a real delivery channel is out of scope (architecture plan non-goals);
// this just needs to be observable for the demo.
public class NotificationStore
{
    private readonly ConcurrentQueue<Notification> _notifications = new();

    public void Add(Notification notification) => _notifications.Enqueue(notification);

    public IReadOnlyList<Notification> GetAll() => _notifications.ToArray();
}
