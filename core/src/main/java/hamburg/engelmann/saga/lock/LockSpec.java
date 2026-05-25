package hamburg.engelmann.saga.lock;

import java.util.List;

/**
 * Declares a set of resources that must be exclusively locked before a saga executes.
 *
 * <p>Implement this interface on a record (or class) whose fields represent the
 * individual resources to lock. Each field type must implement {@link Lockable}:
 *
 * <pre>{@code
 * record OrderProcessingLocks(OrderId orderId, UserId userId) implements LockSpec {
 *     public List<Lockable> lockableResources() { return List.of(orderId, userId); }
 * }
 *
 * Saga.builder("ProcessOrder", Order.class, Receipt.class)
 *     .withLock(input -> new OrderProcessingLocks(input.getOrderId(), input.getUserId()))
 *     .withLock(input -> new InvoiceLocks(input.getInvoiceId()))
 *     ...
 * }</pre>
 *
 * <p>Multiple {@code withLock} calls accumulate — all returned resources are locked before
 * the saga runs.
 */
public interface LockSpec {

    List<? extends Lockable> lockableResources();

}
