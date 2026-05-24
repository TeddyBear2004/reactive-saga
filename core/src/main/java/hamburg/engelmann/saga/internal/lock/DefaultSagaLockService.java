package hamburg.engelmann.saga.internal.lock;

import hamburg.engelmann.saga.lock.LockableResourceId;
import hamburg.engelmann.saga.lock.SagaLock;
import hamburg.engelmann.saga.lock.SagaLockContext;
import hamburg.engelmann.saga.lock.SagaLockRepository;
import hamburg.engelmann.saga.lock.SagaLockService;
import hamburg.engelmann.saga.lock.SagaLockStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Default implementation of {@link SagaLockService} backed by a {@link SagaLockRepository}.
 */
public class DefaultSagaLockService implements SagaLockService {

    private static final Logger log = LoggerFactory.getLogger(DefaultSagaLockService.class);

    private final SagaLockRepository repository;

    public DefaultSagaLockService(SagaLockRepository repository) {
        this.repository = repository;
    }

    @Override
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
                .flatMap(lock -> repository.save(
                        SagaLock.of(lock.getId(), lock.getSagaId(), lock.getSagaName(),
                                    lock.getResourceType(), lock.getResourceId(), lock.getUserId(),
                                    lock.getStartedAt(), now, status)))
                .then();
    }

    private Mono<Void> releaseLocksForSaga(UUID sagaId) {
        return repository.findBySagaId(sagaId)
                .flatMap(lock -> repository.save(
                        SagaLock.of(lock.getId(), lock.getSagaId(), lock.getSagaName(),
                                    lock.getResourceType(), lock.getResourceId(), lock.getUserId(),
                                    lock.getStartedAt(), Instant.now(), SagaLockStatus.RELEASED)))
                .then()
                .onErrorResume(e -> { log.error("Failed to rollback partial locks sagaId={}: {}", sagaId, e.getMessage(), e); return Mono.empty(); });
    }

    private static SagaLock buildLock(SagaLockContext ctx, LockableResourceId res, Instant now) {
        return SagaLock.of(null, ctx.sagaId(), ctx.sagaName(), res.resourceType(), res.resourceId(),
                           ctx.userId(), now, null, SagaLockStatus.ACTIVE);
    }

}
