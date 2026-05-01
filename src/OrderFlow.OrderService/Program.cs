using Confluent.Kafka;
using OrderFlow.Contracts;
using System.Text.Json;
using OrderFlow.OrderService.Orders;
using Microsoft.EntityFrameworkCore;

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
    var order = new Order
    {
        Id = Guid.NewGuid(),
        UserId = request.UserId,
        Amount = request.Amount,
        Currency = request.Currency,
        CreatedAt = DateTimeOffset.UtcNow
    };

    dbContext.Orders.Add(order);

    await dbContext.SaveChangesAsync();

    return Results.Created($"/orders/{order.Id}", order);
})
.WithName("CreateOrder");


app.Run();
