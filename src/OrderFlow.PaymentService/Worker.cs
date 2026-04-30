using Confluent.Kafka;
using OrderFlow.Contracts;
using System.Text.Json;

namespace OrderFlow.PaymentService;

public class Worker(
    ILogger<Worker> logger,
    IConfiguration configuration) : BackgroundService
{

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrapServers = configuration["Kafka:BootstrapServers"];
        var topic = configuration["Kafka:OrderCreatedTopic"];
        var groupId = configuration["Kafka:GroupId"];

        if (string.IsNullOrWhiteSpace(bootstrapServers))
        {
            throw new InvalidOperationException("Kafka:BootstrapServers is not configured.");
        }

        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new InvalidOperationException("Kafka:OrderCreatedTopic is not configured.");
        }

        if (string.IsNullOrWhiteSpace(groupId))
        {
            throw new InvalidOperationException("Kafka:GroupId is not configured.");
        }

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        consumer.Subscribe(topic);

        logger.LogInformation(
            "Kafka consumer started. Topic: {Topic}, GroupId: {GroupId}",
            topic,
            groupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(stoppingToken);

                var orderCreatedEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(
                    consumeResult.Message.Value);

                if (orderCreatedEvent is null)
                {
                    logger.LogWarning(
                        "Received empty or invalid message. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value);

                    continue;
                }



                logger.LogInformation(
                   "Order created event received. EventId: {EventId}, OrderId: {OrderId}, UserId: {UserId}, Amount: {Amount}, Currency: {Currency}, Partition: {Partition}, Offset: {Offset}",
                   orderCreatedEvent.EventId,
                   orderCreatedEvent.OrderId,
                   orderCreatedEvent.UserId,
                   orderCreatedEvent.Amount,
                   orderCreatedEvent.Currency,
                   consumeResult.Partition.Value,
                   consumeResult.Offset.Value);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Kafka consumer is stopping due to cancellation.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred while consuming messages from Kafka.");
        }
        finally
        {
            consumer.Close();
            logger.LogInformation("Kafka consumer has been closed.");
        }
        return Task.CompletedTask;
    }
}

