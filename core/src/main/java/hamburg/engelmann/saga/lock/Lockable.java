package hamburg.engelmann.saga.lock;

/**
 * Marks a domain object as capable of providing a {@link LockableResourceId} for exclusive locking.
 *
 * <p>Implement this interface on entity ID types or domain objects that should participate in
 * saga resource locking. Typically combined with {@link LockSpec} to declare which resources
 * a saga step must lock:
 *
 * <pre>{@code
 * record OrderId(UUID value) implements LockableResourceId {}
 *
 * record OrderLocks(OrderId orderId, UserId userId) implements LockSpec {
 *     public List<Lockable> lockableResources() { return List.of(orderId, userId); }
 * }
 * }</pre>
 *
 * <p>Note: {@link LockableResourceId} already extends this interface, so all existing
 * {@code LockableResourceId} implementations are automatically {@code Lockable}.
 */
public interface Lockable {

    LockableResourceId getId();

}
