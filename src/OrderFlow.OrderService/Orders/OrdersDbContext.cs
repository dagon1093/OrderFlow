using Microsoft.EntityFrameworkCore;

namespace OrderFlow.OrderService.Orders;

public sealed class OrdersDbContext : DbContext
{
    public OrdersDbContext(DbContextOptions<OrdersDbContext> options)
        : base(options)
    {
    }

    public DbSet<Order> Orders => Set<Order>();

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
    }
}