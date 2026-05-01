using Confluent.Kafka;
using OrderFlow.Contracts;
using System.Text.Json;

namespace OrderFlow.PaymentService;

public class Worker(
    ILogger<Worker> logger,
    IConfiguration configuration) : BackgroundService
{

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

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
            EnableAutoCommit = false
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

                ConsumeResult<string, string> consumeResult;

                try
                {
                    consumeResult = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(
                        ex,
                        "Kafka consume error. Reason: {Reason}",
                        ex.Error.Reason);

                    continue;
                }

                OrderCreatedEvent? orderCreatedEvent;

                try
                {
                    orderCreatedEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(
                        consumeResult.Message.Value);
                }
                catch (JsonException ex)
                {
                    logger.LogError(
                        ex,
                        "Failed to deserialize Kafka message. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, Value: {Value}",
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value,
                        consumeResult.Message.Value);

                    // Temporary behavior: skip malformed messages to avoid blocking the partition.
                    // Later this should be replaced with DLQ handling.
                    consumer.Commit(consumeResult);

                    logger.LogWarning(
                        "Invalid Kafka message skipped and offset committed. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value);

                    continue;
                }


                if (orderCreatedEvent is null)
                {
                    logger.LogWarning(
                        "Received empty or invalid message. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value);

                    // Temporary behavior: skip malformed messages to avoid blocking the partition.
                    // Later this should be replaced with DLQ handling.
                    consumer.Commit(consumeResult);

                    logger.LogWarning(
                        "Empty Kafka message skipped and offset committed. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
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

                consumer.Commit(consumeResult);

                logger.LogInformation(
                    "Kafka offset committed. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                    consumeResult.Topic,
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
            throw;
        }
        finally
        {
            consumer.Close();
            logger.LogInformation("Kafka consumer has been closed.");
        }
    }
}

