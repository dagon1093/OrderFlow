using System;
using System.Collections.Generic;
using System.Text;

namespace OrderFlow.Contracts
{
    public sealed record OrderCreatedEvent(
        Guid OrderId,
        DateTimeOffset CreatedAt,
        decimal Amount,
        string Currency
        );
}
