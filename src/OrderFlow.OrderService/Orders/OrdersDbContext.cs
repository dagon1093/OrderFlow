using Microsoft.EntityFrameworkCore;

namespace OrderFlow.OrderService.Orders;

public sealed class OrdersDbContext : DbContext
{
    public OrdersDbContext(DbContextOptions<OrdersDbContext> options)
        : base(options)
    {
    }

    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders");

            entity.HasKey(order => order.Id);

            entity.Property(order => order.Id)
                .HasColumnName("id")
                .IsRequired();

            entity.Property(order => order.UserId)
                .HasColumnName("user_id")
                .IsRequired();

            entity.Property(order => order.Amount)
                .HasColumnName("amount")
                .HasPrecision(18, 2)
                .IsRequired();

            entity.Property(order => order.Currency)
                .HasColumnName("currency")
                .HasMaxLength(3)
                .IsRequired();

            entity.Property(order => order.CreatedAt)
                .HasColumnName("created_at")
                .IsRequired();
        });

        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.ToTable("outbox_messages");

            entity.HasKey(message => message.Id);

            entity.Property(message => message.Id)
                .HasColumnName("id")
                .IsRequired();

            entity.Property(message => message.Type)
                .HasColumnName("type")
                .HasMaxLength(200)
                .IsRequired();

            entity.Property(message => message.Payload)
                .HasColumnName("payload")
                .HasColumnType("jsonb")
                .IsRequired();

            entity.Property(message => message.OccurredAt)
                .HasColumnName("occurred_at")
                .IsRequired();

            entity.Property(message => message.ProcessedAt)
                .HasColumnName("processed_at");

            entity.Property(message => message.Error)
                .HasColumnName("error");

            entity.HasIndex(message => message.ProcessedAt)
                .HasDatabaseName("ix_outbox_messages_processed_at");
        });
    }
}