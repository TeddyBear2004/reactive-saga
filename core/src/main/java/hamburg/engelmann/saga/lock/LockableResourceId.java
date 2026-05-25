package hamburg.engelmann.saga.lock;

import java.util.UUID;

/**
 * Marks an identifier type as eligible for exclusive saga resource locking.
 *
 * <p>Default implementations derive {@link #resourceType()} by stripping the {@code "Id"} suffix
 * from the simple class name (e.g. {@code CloudServerId} → {@code "CloudServer"}),
 * and {@link #resourceId()} delegates to {@code value().toString()}.
 *
 * <p>Extends {@link Lockable}: every {@code LockableResourceId} can be used directly as a
 * component in a {@link LockSpec} record without any additional boilerplate.
 */
public interface LockableResourceId extends Lockable {

    UUID value();

    default String resourceType() {
        String name = getClass().getSimpleName();
        return name.endsWith("Id") ? name.substring(0, name.length() - 2) : name;
    }

    default String resourceId() {
        return value().toString();
    }

    /** Returns {@code this}, since a {@code LockableResourceId} is its own lockable identity. */
    @Override
    default LockableResourceId getId() {
        return this;
    }

}
