package com.saga.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Wraps a reactive saga execution with exclusive per-resource locking.
 *
 * <p>Locks are acquired sequentially before the saga runs and released (or marked FAILED) afterwards.
 * A {@link ResourceAlreadyLockedException} is raised if a resource is already locked.
 */
public class SagaLockService {

    private static final Logger log = LoggerFactory.getLogger(SagaLockService.class);

    private final SagaLockRepository repository;

    public SagaLockService(SagaLockRepository repository) {
        this.repository = repository;
    }

    public <T> Mono<T> withLock(SagaLockContext context, List<LockableResourceId> resources, Mono<T> execution) {
        return Mono.usingWhen(
                acquireLocks(context, resources),
                _locks -> execution,
                this::releaseLocks,
                (locks, _err) -> failLocks(locks),
                this::releaseLocks
        );
    }

    private Mono<List<SagaLock>> acquireLocks(SagaLockContext context, List<LockableResourceId> resources) {
        Instant now = Instant.now();
        return Flux.fromIterable(resources)
                .concatMap(res -> repository.save(buildLock(context, res, now)))
                .collectList()
                .onErrorResume(err -> releaseLocksForSaga(context.sagaId()).then(Mono.error(err)));
    }

    private Mono<Void> releaseLocks(List<SagaLock> locks) {
        return updateLocks(locks, SagaLockStatus.RELEASED)
                .onErrorResume(e -> { log.error("Failed to release locks: {}", e.getMessage(), e); return Mono.empty(); });
    }

    private Mono<Void> failLocks(List<SagaLock> locks) {
        return updateLocks(locks, SagaLockStatus.FAILED)
                .onErrorResume(e -> { log.error("Failed to mark locks FAILED: {}", e.getMessage(), e); return Mono.empty(); });
    }

    private Mono<Void> updateLocks(List<SagaLock> locks, SagaLockStatus status) {
        Instant now = Instant.now();
        return Flux.fromIterable(locks)
                .flatMap(lock -> {
                    lock.setStatus(status);
                    lock.setReleasedAt(now);
                    return repository.save(lock);
                })
                .then();
    }

    private Mono<Void> releaseLocksForSaga(UUID sagaId) {
        return repository.findBySagaId(sagaId)
                .flatMap(lock -> {
                    lock.setStatus(SagaLockStatus.RELEASED);
                    lock.setReleasedAt(Instant.now());
                    return repository.save(lock);
                })
                .then()
                .onErrorResume(e -> { log.error("Failed to rollback partial locks sagaId={}: {}", sagaId, e.getMessage(), e); return Mono.empty(); });
    }

    private static SagaLock buildLock(SagaLockContext ctx, LockableResourceId res, Instant now) {
        SagaLock lock = new SagaLock();
        lock.setSagaId(ctx.sagaId());
        lock.setSagaName(ctx.sagaName());
        lock.setResourceType(res.resourceType());
        lock.setResourceId(res.resourceId());
        lock.setUserId(ctx.userId());
        lock.setStartedAt(now);
        lock.setStatus(SagaLockStatus.ACTIVE);
        return lock;
    }

}
