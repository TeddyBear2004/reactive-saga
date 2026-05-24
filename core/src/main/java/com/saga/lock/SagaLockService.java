package com.saga.lock;

import com.saga.internal.lock.DefaultSagaLockService;
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

    static SagaLockService create(SagaLockRepository repository) {
        return new DefaultSagaLockService(repository);
    }

}
