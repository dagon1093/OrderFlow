using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OrderFlow.Contracts;
using OrderFlow.OrderService.Orders;
using OrderFlow.OrderService.Outbox;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.AddDbContext<OrdersDbContext>(options =>
{
    var connectionString = builder.Configuration.GetConnectionString("OrdersDatabase");
    if (string.IsNullOrWhiteSpace(connectionString))
    {
        throw new InvalidOperationException("Connection string 'OrdersDatabase' is not configured.");
    }
    options.UseNpgsql(connectionString);
});

builder.Services.AddSingleton<IProducer<string, string>>(_ =>
{
    var bootstrapServers = builder.Configuration["Kafka:BootstrapServers"];

    if (string.IsNullOrWhiteSpace(bootstrapServers))
    {
        throw new InvalidOperationException("Kafka:BootstrapServers is not configured.");
    }

    var producerConfig = new ProducerConfig
    {
        BootstrapServers = bootstrapServers
    };

    return new ProducerBuilder<string, string>(producerConfig).Build();
});

builder.Services.AddHostedService<OutboxPublisher>();


var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// Disabled for local HTTP testing with curl/Postman.
// app.UseHttpsRedirection();

app.MapPost("/orders/test-event", async (
    IProducer<string, string> producer,
    IConfiguration configuration,
    ILogger<Program> logger) =>
{
    var topic = configuration["Kafka:OrderCreatedTopic"];

    if (string.IsNullOrWhiteSpace(topic))
    {
        return Results.Problem("Kafka:OrderCreatedTopic is not configured.");
    }

    var orderCreatedEvent = new OrderCreatedEvent(
        EventId: Guid.NewGuid(),
        OrderId: Guid.NewGuid(),
        UserId: Guid.NewGuid(),
        CreatedAt: DateTimeOffset.UtcNow,
        Amount: 1000m,
        Currency: "RUB");

    var key = orderCreatedEvent.OrderId.ToString();

    var value = JsonSerializer.Serialize(orderCreatedEvent);

    var deliveryResult = await producer.ProduceAsync(
        topic,
        new Message<string, string>
        {
            Key = key,
            Value = value
        });

    logger.LogInformation(
        "Kafka message delivered. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, Status: {Status}, Key: {Key}",
        deliveryResult.Topic,
        deliveryResult.Partition.Value,
        deliveryResult.Offset.Value,
        deliveryResult.Status,
        key);

    return Results.Ok(new
    {
        Message = "Order created event was sent to Kafka",
        Topic = deliveryResult.Topic,
        Partition = deliveryResult.Partition.Value,
        Offset = deliveryResult.Offset.Value,
        Status = deliveryResult.Status.ToString(),
        Key = key,
        Event = orderCreatedEvent
    });
})
.WithName("SendOrderCreatedTestEvent");

app.MapPost("/orders", async (
    CreateOrderRequest request,
    OrdersDbContext dbContext) =>
{
    var now = DateTimeOffset.UtcNow;

    var order = new Order
    {
        Id = Guid.NewGuid(),
        UserId = request.UserId,
        Amount = request.Amount,
        Currency = request.Currency,
        CreatedAt = now
    };

    var orderCreatedEvent = new OrderCreatedEvent(
        EventId: Guid.NewGuid(),
        OrderId: order.Id,
        UserId: order.UserId,
        CreatedAt: order.CreatedAt,
        Amount: order.Amount,
        Currency: order.Currency);

    var outboxMessage = new OutboxMessage
    {
        Id = orderCreatedEvent.EventId,
        Type = nameof(OrderCreatedEvent),
        Payload = JsonSerializer.Serialize(orderCreatedEvent),
        OccurredAt = now
    };

    dbContext.Orders.Add(order);
    dbContext.OutboxMessages.Add(outboxMessage);

    await dbContext.SaveChangesAsync();

    return Results.Created($"/orders/{order.Id}", order);
})
.WithName("CreateOrder");


app.Run();
