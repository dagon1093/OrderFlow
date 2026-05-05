namespace OrderFlow.OrderService.Orders
{
    public sealed class OutboxMessage
    {
        public Guid Id { get; set; }

        public string Type {  get; set; } = string.Empty;

        public string Payload { get; set; } = string.Empty;

        public DateTimeOffset OccurredAt { get; set; } 

        public DateTimeOffset? ProcessedAt { get; set; }

        public string? Error { get; set; }
    }
}
