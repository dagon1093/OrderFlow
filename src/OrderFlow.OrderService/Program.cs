using Confluent.Kafka;
using OrderFlow.Contracts;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();


builder.Services.AddOpenApi();

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
        OrderId: Guid.NewGuid(),
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


app.Run();
