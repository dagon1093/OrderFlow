namespace OrderFlow.OrderService.Orders
{
    public sealed record CreateOrderRequest(
        Guid UserId,
        decimal Amount,
        string Currency);
}
