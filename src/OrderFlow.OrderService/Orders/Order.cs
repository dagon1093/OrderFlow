namespace OrderFlow.OrderService.Orders
{
    public sealed class Order
    {
        public Guid Id { get; set; }
        public Guid UserId { get; set; }
        public decimal Amount { get; set; }
        public string Currency { get; set; } = string.Empty;
        public DateTimeOffset CreatedAt { get; set; }
    }
}
