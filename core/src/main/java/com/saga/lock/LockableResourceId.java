package com.saga.lock;

import java.util.UUID;

/**
 * Marks an identifier type as eligible for exclusive saga resource locking.
 *
 * <p>Default implementations derive {@link #resourceType()} by stripping the {@code "Id"} suffix
 * from the simple class name (e.g. {@code CloudServerId} → {@code "CloudServer"}),
 * and {@link #resourceId()} delegates to {@code value().toString()}.
 */
public interface LockableResourceId {

    UUID value();

    default String resourceType() {
        String name = getClass().getSimpleName();
        return name.endsWith("Id") ? name.substring(0, name.length() - 2) : name;
    }

    default String resourceId() {
        return value().toString();
    }

}
