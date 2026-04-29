namespace OrderFlow.Contracts
{
    public sealed record OrderCreatedEvent(
        Guid EventId,
        Guid OrderId,
        Guid UserId,
        DateTimeOffset CreatedAt,
        decimal Amount,
        string Currency
        );
}
