package hamburg.engelmann.saga.lock;

import hamburg.engelmann.saga.internal.lock.DefaultSagaLockService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultSagaLockServiceTest {

    // ── stub repository ───────────────────────────────────────────────────────

    static class StubRepository implements SagaLockRepository {
        final CopyOnWriteArrayList<SagaLock> store = new CopyOnWriteArrayList<>();

        @Override
        public Mono<SagaLock> save(SagaLock lock) {
            SagaLockId id = lock.getId() != null ? lock.getId() : new SagaLockId(UUID.randomUUID());
            SagaLock stored = SagaLock.of(
                    id, lock.getSagaId(), lock.getSagaName(),
                    lock.getResourceType(), lock.getResourceId(), lock.getUserId(),
                    lock.getStartedAt(), lock.getReleasedAt(), lock.getStatus());
            store.removeIf(l -> id.equals(l.getId()));
            store.add(stored);
            return Mono.just(stored);
        }

        @Override
        public Flux<SagaLock> findBySagaId(UUID sagaId) {
            return Flux.fromIterable(store).filter(l -> l.getSagaId().equals(sagaId));
        }

        List<SagaLock> locksWithStatus(SagaLockStatus status) {
            return store.stream().filter(l -> l.getStatus() == status).toList();
        }
    }

    /** Repository whose release (RELEASED-status) saves always fail. */
    static class FailingReleaseRepository extends StubRepository {
        @Override
        public Mono<SagaLock> save(SagaLock lock) {
            if (lock.getStatus() == SagaLockStatus.RELEASED) {
                return Mono.error(new RuntimeException("release save failed"));
            }
            return super.save(lock);
        }
    }

    /** Repository whose fail-mark (FAILED-status) saves always fail. */
    static class FailingFailMarkRepository extends StubRepository {
        @Override
        public Mono<SagaLock> save(SagaLock lock) {
            if (lock.getStatus() == SagaLockStatus.FAILED) {
                return Mono.error(new RuntimeException("fail-mark save failed"));
            }
            return super.save(lock);
        }
    }

    /** Repository that rejects ACTIVE saves for a configured resource id. */
    static class AcquisitionFailingRepository extends StubRepository {
        private final String blockedResourceId;

        AcquisitionFailingRepository(String blockedResourceId) {
            this.blockedResourceId = blockedResourceId;
        }

        @Override
        public Mono<SagaLock> save(SagaLock lock) {
            if (lock.getStatus() == SagaLockStatus.ACTIVE
                    && blockedResourceId.equals(lock.getResourceId())) {
                return Mono.error(new ResourceAlreadyLockedException(
                        lock.getResourceType(), lock.getResourceId()));
            }
            return super.save(lock);
        }
    }

    /** Repository that fails both ACTIVE acquisitions and rollback saves. */
    static class FailingAcquisitionAndRollbackRepository extends StubRepository {
        private final String blockedResourceId;

        FailingAcquisitionAndRollbackRepository(String blockedResourceId) {
            this.blockedResourceId = blockedResourceId;
        }

        @Override
        public Mono<SagaLock> save(SagaLock lock) {
            if (lock.getStatus() == SagaLockStatus.ACTIVE
                    && blockedResourceId.equals(lock.getResourceId())) {
                return Mono.error(new ResourceAlreadyLockedException(
                        lock.getResourceType(), lock.getResourceId()));
            }
            // Also fail rollback releases for this saga (simulates rollback save error)
            if (lock.getStatus() == SagaLockStatus.RELEASED) {
                return Mono.error(new RuntimeException("rollback release save failed"));
            }
            return super.save(lock);
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    record ItemId(UUID value) implements LockableResourceId {}

    static LockableResourceId resource(String uuidString) {
        return new ItemId(UUID.fromString(uuidString));
    }

    static final String RES_1 = "00000000-0000-0000-0000-000000000001";
    static final String RES_2 = "00000000-0000-0000-0000-000000000002";

    StubRepository repo;
    DefaultSagaLockService service;
    SagaLockContext ctx;

    @BeforeEach
    void setUp() {
        repo = new StubRepository();
        service = new DefaultSagaLockService(repo);
        ctx = SagaLockContext.forSaga("test-saga");
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void withLock_executionSucceeds_lockReleased() {
        StepVerifier.create(
                        service.withLock(ctx, List.of(resource(RES_1)), Mono.just("ok"))
                )
                .assertNext(r -> assertThat(r).isEqualTo("ok"))
                .verifyComplete();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED)).hasSize(1);
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED).getFirst().getResourceId())
                .isEqualTo(RES_1);
    }

    @Test
    void withLock_multipleResources_allLocksReleased() {
        List<LockableResourceId> resources = List.of(resource(RES_1), resource(RES_2));

        StepVerifier.create(service.withLock(ctx, resources, Mono.just("ok")))
                .assertNext(r -> assertThat(r).isEqualTo("ok"))
                .verifyComplete();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED)).hasSize(2);
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED))
                .extracting(SagaLock::getResourceId)
                .containsExactlyInAnyOrder(RES_1, RES_2);
    }

    @Test
    void withLock_executionFails_locksMarkedFailed() {
        RuntimeException executionError = new RuntimeException("execution failed");

        StepVerifier.create(
                        service.withLock(ctx, List.of(resource(RES_1)), Mono.error(executionError))
                )
                .expectErrorMessage("execution failed")
                .verify();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.FAILED)).hasSize(1);
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED)).isEmpty();
    }

    @Test
    void withLock_emptyResourceList_executionRunsWithoutLocks() {
        StepVerifier.create(
                        service.withLock(ctx, List.of(), Mono.just("done"))
                )
                .assertNext(r -> assertThat(r).isEqualTo("done"))
                .verifyComplete();

        assertThat(repo.store).isEmpty();
    }

    @Test
    void withLock_acquisitionFails_partialLocksReleasedAndErrorPropagated() {
        // First resource acquires fine; second is already locked
        AcquisitionFailingRepository failRepo = new AcquisitionFailingRepository(RES_2);
        DefaultSagaLockService failService = new DefaultSagaLockService(failRepo);

        List<LockableResourceId> resources = List.of(resource(RES_1), resource(RES_2));

        StepVerifier.create(failService.withLock(ctx, resources, Mono.just("ok")))
                .expectError(ResourceAlreadyLockedException.class)
                .verify();

        // First lock should have been rolled back (RELEASED)
        assertThat(failRepo.locksWithStatus(SagaLockStatus.RELEASED)).hasSize(1);
        assertThat(failRepo.locksWithStatus(SagaLockStatus.RELEASED).getFirst().getResourceId())
                .isEqualTo(RES_1);
        assertThat(failRepo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
    }

    @Test
    void withLock_releaseSaveFails_sagaResultStillSucceeds() {
        FailingReleaseRepository failRepo = new FailingReleaseRepository();
        DefaultSagaLockService failService = new DefaultSagaLockService(failRepo);

        // Release fails, but the saga execution result must still succeed
        StepVerifier.create(
                        failService.withLock(ctx, List.of(resource(RES_1)), Mono.just("result"))
                )
                .assertNext(r -> assertThat(r).isEqualTo("result"))
                .verifyComplete();
    }

    @Test
    void withLock_failMarkSaveFails_executionErrorStillPropagates() {
        FailingFailMarkRepository failRepo = new FailingFailMarkRepository();
        DefaultSagaLockService failService = new DefaultSagaLockService(failRepo);

        RuntimeException executionError = new RuntimeException("exec error");

        // Marking FAILED fails, but the original execution error must still propagate
        StepVerifier.create(
                        failService.withLock(ctx, List.of(resource(RES_1)), Mono.error(executionError))
                )
                .expectErrorMessage("exec error")
                .verify();
    }

    @Test
    void withLock_acquisitionFailsAndRollbackFails_originalErrorPropagates() {
        FailingAcquisitionAndRollbackRepository failRepo =
                new FailingAcquisitionAndRollbackRepository(RES_2);
        DefaultSagaLockService failService = new DefaultSagaLockService(failRepo);

        List<LockableResourceId> resources = List.of(resource(RES_1), resource(RES_2));

        // Even though rollback also fails, the original acquisition error must propagate
        StepVerifier.create(failService.withLock(ctx, resources, Mono.just("ok")))
                .expectError(ResourceAlreadyLockedException.class)
                .verify();
    }

    @Test
    void withLock_lockHasCorrectMetadata() {
        SagaLockContext namedCtx = SagaLockContext.forSaga("order-saga").withUserId("user-99");

        StepVerifier.create(
                        new DefaultSagaLockService(repo)
                                .withLock(namedCtx, List.of(resource(RES_1)), Mono.just("x"))
                )
                .assertNext(_ -> {})
                .verifyComplete();

        SagaLock lock = repo.store.getFirst();
        assertThat(lock.getSagaName()).isEqualTo("order-saga");
        assertThat(lock.getSagaId()).isEqualTo(namedCtx.sagaId());
        assertThat(lock.getUserId()).isEqualTo("user-99");
        assertThat(lock.getResourceId()).isEqualTo(RES_1);
        assertThat(lock.getStartedAt()).isNotNull();
    }
}
