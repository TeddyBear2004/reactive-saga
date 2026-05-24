package hamburg.engelmann.saga.lock;

import hamburg.engelmann.saga.internal.DefaultSagaLock;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a distributed lock acquired during saga execution.
 *
 * <p>Instances are created by {@link SagaLockService} internally.
 * Infrastructure implementations of {@link SagaLockRepository} receive and return
 * this interface; they may use their own persistence entity as long as it implements it.
 */
public interface SagaLock {

    @Nullable SagaLockId getId();

    UUID getSagaId();

    String getSagaName();

    String getResourceType();

    String getResourceId();

    @Nullable String getUserId();

    Instant getStartedAt();

    @Nullable Instant getReleasedAt();

    SagaLockStatus getStatus();

    /** Creates an immutable {@link SagaLock} value. Used internally by the saga engine. */
    static SagaLock of(@Nullable SagaLockId id, UUID sagaId, String sagaName,
                       String resourceType, String resourceId, @Nullable String userId,
                       Instant startedAt, @Nullable Instant releasedAt, SagaLockStatus status) {
        return new DefaultSagaLock(id, sagaId, sagaName, resourceType,
                                   resourceId, userId, startedAt, releasedAt, status);
    }

}
