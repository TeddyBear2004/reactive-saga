package hamburg.engelmann.saga.lock;

import hamburg.engelmann.saga.internal.lock.DefaultSagaLockService;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Wraps a reactive saga execution with exclusive per-resource locking.
 *
 * <p>Locks are acquired sequentially before the saga runs and released (or marked FAILED) afterwards.
 * A {@link ResourceAlreadyLockedException} is raised if a resource is already locked.
 *
 * <p>Create instances via the factory:
 * <pre>{@code
 * SagaLockService lockService = SagaLockService.create(myRepository);
 * }</pre>
 */
public interface SagaLockService {

    <T> Mono<T> withLock(SagaLockContext context, List<LockableResourceId> resources, Mono<T> execution);

    /**
     * Acquires locks for the given resources and returns the acquired {@link SagaLock} records.
     * The caller is responsible for releasing or failing the locks via
     * {@link #releaseAcquiredLocks} / {@link #failAcquiredLocks}.
     */
    Mono<List<SagaLock>> acquireLocks(SagaLockContext context, List<LockableResourceId> resources);

    /**
     * Marks the given locks as {@link SagaLockStatus#RELEASED}.
     * Errors during release are logged but do not propagate.
     */
    Mono<Void> releaseAcquiredLocks(List<SagaLock> locks);

    /**
     * Marks the given locks as {@link SagaLockStatus#FAILED}.
     * Errors during update are logged but do not propagate.
     */
    Mono<Void> failAcquiredLocks(List<SagaLock> locks);

    static SagaLockService create(SagaLockRepository repository) {
        return new DefaultSagaLockService(repository);
    }

}
