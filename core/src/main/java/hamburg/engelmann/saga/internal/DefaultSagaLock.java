package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.lock.SagaLock;
import hamburg.engelmann.saga.lock.SagaLockId;
import hamburg.engelmann.saga.lock.SagaLockStatus;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable implementation of {@link SagaLock}.
 * Equality is based on {@link SagaLockId} only, matching the original mutable entity contract.
 */
@Getter
@AllArgsConstructor
@EqualsAndHashCode(of = "id")
@ToString
public class DefaultSagaLock implements SagaLock {

    private final @Nullable SagaLockId id;
    private final UUID sagaId;
    private final String sagaName;
    private final String resourceType;
    private final String resourceId;
    private final @Nullable String userId;
    private final Instant startedAt;
    private final @Nullable Instant releasedAt;
    private final SagaLockStatus status;

}
