using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace BeanNotifs.Messaging;

public record ConsumerBinding(string QueueName, string RoutingKey, Func<EventEnvelope, Task> Handler);

// Bounded retry without a dead-letter exchange: on handler failure the message is
// ack'd (removed) and manually re-published to the same routing key with an
// incremented x-retry-count header. After MaxDeliveryAttempts it's logged and
// dropped instead of re-published. The count lives in the message itself, so it
// survives a pod restart (unlike an in-memory counter keyed by delivery tag).
public class EventConsumerHostedService : BackgroundService
{
    private const int MaxDeliveryAttempts = 5;
    private const string RetryCountHeader = "x-retry-count";

    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly IReadOnlyList<ConsumerBinding> _bindings;
    private readonly ILogger<EventConsumerHostedService> _logger;

    public EventConsumerHostedService(
        IOptions<RabbitMqOptions> options,
        IReadOnlyList<ConsumerBinding> bindings,
        ILogger<EventConsumerHostedService> logger)
    {
        _bindings = bindings;
        _logger = logger;

        var o = options.Value;
        var factory = new ConnectionFactory
        {
            HostName = o.Host,
            Port = o.Port,
            UserName = o.User,
            Password = o.Pass,
            AutomaticRecoveryEnabled = true,
            DispatchConsumersAsync = true, // required for AsyncEventingBasicConsumer's Received handler to actually be awaited
        };

        _connection = factory.CreateConnection("BeanNotifs-consumer");
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(RabbitMqOptions.ExchangeName, ExchangeType.Topic, durable: true);
        _channel.BasicQos(prefetchSize: 0, prefetchCount: 10, global: false);

        foreach (var binding in _bindings)
        {
            _channel.QueueDeclare(binding.QueueName, durable: true, exclusive: false, autoDelete: false);
            _channel.QueueBind(binding.QueueName, RabbitMqOptions.ExchangeName, binding.RoutingKey);
        }
    }

    // No EventPublisher exists in this service (consumer-only, per Q1), so the
    // readiness check probes this connection directly instead.
    public bool IsConnected => _connection.IsOpen && _channel.IsOpen;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        foreach (var binding in _bindings)
        {
            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.Received += (_, ea) => HandleDeliveryAsync(binding, ea);
            _channel.BasicConsume(binding.QueueName, autoAck: false, consumer: consumer);
        }

        return Task.CompletedTask;
    }

    private async Task HandleDeliveryAsync(ConsumerBinding binding, BasicDeliverEventArgs ea)
    {
        EventEnvelope envelope;
        try
        {
            envelope = JsonSerializer.Deserialize<EventEnvelope>(ea.Body.Span)!;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Malformed message on {Queue}, dropping", binding.QueueName);
            _channel.BasicAck(ea.DeliveryTag, multiple: false);
            return;
        }

        try
        {
            await binding.Handler(envelope);
            _channel.BasicAck(ea.DeliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            var attempt = GetRetryCount(ea.BasicProperties) + 1;
            _channel.BasicAck(ea.DeliveryTag, multiple: false); // original delivery is done either way

            if (attempt >= MaxDeliveryAttempts)
            {
                _logger.LogError(ex,
                    "Handler failed {Attempt} times for {EventType} (order {OrderId}) on {Queue} — dropping",
                    attempt, envelope.EventType, envelope.OrderId, binding.QueueName);
                return;
            }

            _logger.LogWarning(ex,
                "Handler failed (attempt {Attempt}/{Max}) for {EventType} (order {OrderId}) on {Queue} — requeueing",
                attempt, MaxDeliveryAttempts, envelope.EventType, envelope.OrderId, binding.QueueName);

            var props = _channel.CreateBasicProperties();
            props.Persistent = true;
            props.ContentType = "application/json";
            props.Headers = new Dictionary<string, object> { [RetryCountHeader] = attempt };

            _channel.BasicPublish(RabbitMqOptions.ExchangeName, binding.RoutingKey, props, ea.Body);
        }
    }

    private static int GetRetryCount(IBasicProperties properties)
    {
        if (properties.Headers is not null && properties.Headers.TryGetValue(RetryCountHeader, out var value))
        {
            return value switch
            {
                int i => i,
                long l => (int)l,
                byte[] bytes => int.Parse(Encoding.UTF8.GetString(bytes)),
                _ => 0,
            };
        }

        return 0;
    }

    public override void Dispose()
    {
        _channel.Dispose();
        _connection.Dispose();
        base.Dispose();
        GC.SuppressFinalize(this);
    }
}
