using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OrderFlow.OrderService.Orders;

namespace OrderFlow.OrderService.Outbox;

public sealed class OutboxPublisher : BackgroundService
{
    private const int BatchSize = 50;
    private static readonly TimeSpan EmptyDelay = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan Delay = TimeSpan.FromMilliseconds(10);

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<OutboxPublisher> _logger;
    private readonly string _orderCreatedTopic;

    public OutboxPublisher(
        IServiceScopeFactory scopeFactory,
        IProducer<string, string> producer,
        ILogger<OutboxPublisher> logger,
        IConfiguration configuration)
    {
        _scopeFactory = scopeFactory;
        _producer = producer;
        _logger = logger;
        _orderCreatedTopic = configuration["Kafka:OrderCreatedTopic"]
        ?? throw new InvalidOperationException("Kafka:OrderCreatedTopic is not configured");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var messagesCount = 0;
            try
            {
                messagesCount = await PublishBatchAsync(stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while publishing outbox messages");
            }

            if (messagesCount == 0)
            {
                await Task.Delay(EmptyDelay, stoppingToken);
            }
            else
            {
                await Task.Delay(Delay, stoppingToken);
            }
            
        }
    }

    private async Task<int> PublishBatchAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();

        var dbContext = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();

        var stopwatch = Stopwatch.StartNew();
        
        var messages = await dbContext.OutboxMessages
            .Where(message => message.ProcessedAt == null)
            .OrderBy(message => message.OccurredAt)
            .Take(BatchSize)
            .ToListAsync(cancellationToken);
        
        var batchReadMs = stopwatch.ElapsedMilliseconds;

        if (messages.Count == 0)
        {
            return 0;
        }

        foreach (var message in messages)
        {
            try
            {
                await _producer.ProduceAsync(
                    _orderCreatedTopic,
                    new Message<string, string>
                    {
                        Key = message.Id.ToString(),
                        Value = message.Payload
                    },
                    cancellationToken);

                message.ProcessedAt = DateTimeOffset.UtcNow;
                message.Error = null;
            }
            catch (Exception ex)
            {
                message.Error = ex.Message;

                _logger.LogError(
                    ex,
                    "Failed to publish outbox message {MessageId}",
                    message.Id);
            }
        }
        
        var publishLoopMs = stopwatch.ElapsedMilliseconds -  batchReadMs;

        await dbContext.SaveChangesAsync(cancellationToken);
        stopwatch.Stop();
        
        var saveChangesMs = stopwatch.ElapsedMilliseconds - batchReadMs -  publishLoopMs;
        
        _logger.LogInformation(
            "Outbox batch processed: BatchSize={BatchSize}, " +
            "BatchReadMs={BatchReadMs}, PublishLoopMs={PublishLoopMs}, " +
            "SaveChangesMs={SaveChangesMs}, DurationMs={DurationMs}",
            messages.Count,
            batchReadMs,
            publishLoopMs,
            saveChangesMs,
            stopwatch.ElapsedMilliseconds);
        
        return messages.Count;
    }
}