package hamburg.engelmann.saga;

import hamburg.engelmann.saga.lock.LockSpec;
import hamburg.engelmann.saga.lock.LockableResourceId;
import hamburg.engelmann.saga.lock.Lockable;
import hamburg.engelmann.saga.lock.ResourceAlreadyLockedException;
import hamburg.engelmann.saga.lock.SagaLock;
import hamburg.engelmann.saga.lock.SagaLockId;
import hamburg.engelmann.saga.lock.SagaLockRepository;
import hamburg.engelmann.saga.lock.SagaLockService;
import hamburg.engelmann.saga.lock.SagaLockStatus;
import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class SagaWithLockTest {

    // ── domain types ──────────────────────────────────────────────────────────

    record OrderId(UUID value) implements LockableResourceId {}

    record OrderInput(OrderId orderId, String name) {}

    record OrderLocks(OrderId orderId) implements LockSpec {
        @Override
        public List<? extends Lockable> lockableResources() {
            return List.of(orderId);
        }
    }

    // ── stub repository ───────────────────────────────────────────────────────

    static class StubLockRepository implements SagaLockRepository {
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

    /** Rejects ACTIVE saves for a pre-configured resource id. */
    static class AlreadyLockedRepository extends StubLockRepository {
        private final String blockedResourceId;

        AlreadyLockedRepository(String blockedResourceId) {
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

    // ── helpers ───────────────────────────────────────────────────────────────

    static SagaStep<OrderInput, String, Void> processStep() {
        return new SagaStep<>() {
            @Override public String name() { return "process"; }
            @Override public Class<OrderInput> inputType() { return OrderInput.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(OrderInput input) {
                return Mono.just(StepResult.stateless("processed-" + input.name()));
            }
        };
    }

    static SagaStep<OrderInput, String, Void> failingProcessStep() {
        return new SagaStep<>() {
            @Override public String name() { return "process"; }
            @Override public Class<OrderInput> inputType() { return OrderInput.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(OrderInput input) {
                return Mono.error(new RuntimeException("processing failed"));
            }
        };
    }

    StubLockRepository repo;
    SagaLockService lockService;
    UUID orderId;
    OrderInput orderInput;

    @BeforeEach
    void setUp() {
        repo = new StubLockRepository();
        lockService = SagaLockService.create(repo);
        orderId = UUID.randomUUID();
        orderInput = new OrderInput(new OrderId(orderId), "test-order");
    }

    // ── withLock(Function) ────────────────────────────────────────────────────

    @Test
    void withLock_function_executionSucceeds_lockAcquiredAndReleased() {
        Saga<OrderInput, String> saga = Saga.builder("order-saga", OrderInput.class, String.class)
                .withLock(input -> new OrderLocks(input.orderId()))
                .withSagaLockService(lockService)
                .step(processStep())
                .build();

        StepVerifier.create(saga.execute(orderInput))
                .assertNext(r -> assertThat(r).isEqualTo("processed-test-order"))
                .verifyComplete();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED)).hasSize(1);
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED).getFirst().getResourceId())
                .isEqualTo(orderId.toString());
    }

    @Test
    void withLock_function_sagaFails_locksMarkedFailed() {
        Saga<OrderInput, String> saga = Saga.builder("order-saga", OrderInput.class, String.class)
                .withLock(input -> new OrderLocks(input.orderId()))
                .withSagaLockService(lockService)
                .step(failingProcessStep())
                .build();

        StepVerifier.create(saga.execute(orderInput))
                .expectErrorMessage("processing failed")
                .verify();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.FAILED)).hasSize(1);
    }

    @Test
    void withLock_function_resourceAlreadyLocked_propagatesError() {
        AlreadyLockedRepository lockedRepo = new AlreadyLockedRepository(orderId.toString());
        SagaLockService lockedService = SagaLockService.create(lockedRepo);

        Saga<OrderInput, String> saga = Saga.builder("order-saga", OrderInput.class, String.class)
                .withLock(input -> new OrderLocks(input.orderId()))
                .withSagaLockService(lockedService)
                .step(processStep())
                .build();

        StepVerifier.create(saga.execute(orderInput))
                .expectError(ResourceAlreadyLockedException.class)
                .verify();
    }

    @Test
    void withLock_multipleWithLockCalls_allResourcesLocked() {
        record UserId(UUID value) implements LockableResourceId {}
        record UserLocks(UserId userId) implements LockSpec {
            @Override public List<? extends Lockable> lockableResources() { return List.of(userId); }
        }

        UUID userId = UUID.randomUUID();
        record FullInput(OrderId orderId, UserId userId) {}

        SagaStep<FullInput, String, Void> step = new SagaStep<>() {
            @Override public String name() { return "s"; }
            @Override public Class<FullInput> inputType() { return FullInput.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(FullInput input) {
                return Mono.just(StepResult.stateless("done"));
            }
        };

        Saga<FullInput, String> saga = Saga.builder("multi-lock-saga", FullInput.class, String.class)
                .withLock(input -> new OrderLocks(input.orderId()))
                .withLock(input -> new UserLocks(input.userId()))
                .withSagaLockService(lockService)
                .step(step)
                .build();

        FullInput fullInput = new FullInput(new OrderId(orderId), new UserId(userId));
        StepVerifier.create(saga.execute(fullInput))
                .assertNext(r -> assertThat(r).isEqualTo("done"))
                .verifyComplete();

        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED)).hasSize(2);
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED))
                .extracting(SagaLock::getResourceId)
                .containsExactlyInAnyOrder(orderId.toString(), userId.toString());
    }

    // ── withLock(Class) ───────────────────────────────────────────────────────

    @Test
    void withLock_class_resolvesLockSpecFromInput() {
        // OrderLocks(orderId) is mapped from OrderInput(orderId, name) via DI
        Saga<OrderInput, String> saga = Saga.builder("order-saga", OrderInput.class, String.class)
                .withLock(OrderLocks.class)
                .withSagaLockService(lockService)
                .step(processStep())
                .build();

        StepVerifier.create(saga.execute(orderInput))
                .assertNext(r -> assertThat(r).isEqualTo("processed-test-order"))
                .verifyComplete();

        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED)).hasSize(1);
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED).getFirst().getResourceId())
                .isEqualTo(orderId.toString());
    }

    @Test
    void withLock_class_sagaFails_locksMarkedFailed() {
        Saga<OrderInput, String> saga = Saga.builder("order-saga", OrderInput.class, String.class)
                .withLock(OrderLocks.class)
                .withSagaLockService(lockService)
                .step(failingProcessStep())
                .build();

        StepVerifier.create(saga.execute(orderInput))
                .expectErrorMessage("processing failed")
                .verify();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.FAILED)).hasSize(1);
    }

    // ── no lock service — lock spec ignored ───────────────────────────────────

    @Test
    void withLock_noLockService_executionRunsWithoutLocks() {
        // withSagaLockService is NOT called — lock spec is silently ignored
        Saga<OrderInput, String> saga = Saga.builder("order-saga", OrderInput.class, String.class)
                .withLock(input -> new OrderLocks(input.orderId()))
                // no .withSagaLockService(...)
                .step(processStep())
                .build();

        StepVerifier.create(saga.execute(orderInput))
                .assertNext(r -> assertThat(r).isEqualTo("processed-test-order"))
                .verifyComplete();

        assertThat(repo.store).isEmpty();
    }
}
