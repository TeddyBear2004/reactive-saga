package hamburg.engelmann.saga.lock;

import org.jspecify.annotations.Nullable;

import java.util.UUID;

/**
 * Immutable context describing the saga that is acquiring resource locks.
 *
 * <pre>{@code
 * SagaLockContext ctx = SagaLockContext.forSaga("ServerProvisioning")
 *         .withUserId(input.userId());
 * }</pre>
 */
public record SagaLockContext(UUID sagaId, String sagaName, @Nullable String userId) {

    public static SagaLockContext forSaga(String sagaName) {
        return new SagaLockContext(UUID.randomUUID(), sagaName, null);
    }

    public SagaLockContext withUserId(@Nullable String userId) {
        return new SagaLockContext(sagaId, sagaName, userId);
    }

}
