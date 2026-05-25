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
import hamburg.engelmann.saga.lock.SagaStepContext;
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

/**
 * Tests for step-level locking via {@link SagaStepContext}.
 * Verifies that locks acquired mid-step are held by the saga until saga completion.
 */
class SagaStepLockTest {

    // ── domain types ──────────────────────────────────────────────────────────

    record ResourceId(UUID value) implements LockableResourceId {}

    record Input(String name) {}

    record ResourceLock(ResourceId resourceId) implements LockSpec {
        @Override
        public List<? extends Lockable> lockableResources() {
            return List.of(resourceId);
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

    // ── fixtures ──────────────────────────────────────────────────────────────

    StubLockRepository repo;
    SagaLockService lockService;
    UUID resourceUuid;
    Input input;

    @BeforeEach
    void setUp() {
        repo = new StubLockRepository();
        lockService = SagaLockService.create(repo);
        resourceUuid = UUID.randomUUID();
        input = new Input("test");
    }

    // ── basic mid-step acquire ────────────────────────────────────────────────

    @Test
    void stepContext_lockAcquiredMidStep_releasedOnSuccess() {
        ResourceId resourceId = new ResourceId(resourceUuid);

        SagaStep<Input, String, Void> step = new SagaStep<>() {
            @Override public String name() { return "step"; }
            @Override public Class<Input> inputType() { return Input.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(Input in) { throw new UnsupportedOperationException(); }

            @Override
            public Mono<StepResult<String, Void>> execute(Input in, SagaStepContext ctx) {
                // Simulates: resource ID only known after async lookup
                return Mono.just(resourceId)
                        .flatMap(id -> ctx.acquireLock(new ResourceLock(id)).thenReturn(id))
                        .map(id -> StepResult.stateless("done-" + in.name()));
            }
        };

        Saga<Input, String> saga = Saga.builder("test-saga", Input.class, String.class)
                .withSagaLockService(lockService)
                .step(step)
                .build();

        StepVerifier.create(saga.execute(input))
                .assertNext(r -> assertThat(r).isEqualTo("done-test"))
                .verifyComplete();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED)).hasSize(1);
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED).getFirst().getResourceId())
                .isEqualTo(resourceUuid.toString());
    }

    @Test
    void stepContext_sagaFails_acquiredLocksMarkedFailed() {
        ResourceId resourceId = new ResourceId(resourceUuid);

        SagaStep<Input, String, Void> step = new SagaStep<>() {
            @Override public String name() { return "step"; }
            @Override public Class<Input> inputType() { return Input.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(Input in) { throw new UnsupportedOperationException(); }

            @Override
            public Mono<StepResult<String, Void>> execute(Input in, SagaStepContext ctx) {
                return ctx.acquireLock(new ResourceLock(resourceId))
                        .then(Mono.error(new RuntimeException("step failed")));
            }
        };

        Saga<Input, String> saga = Saga.builder("test-saga", Input.class, String.class)
                .withSagaLockService(lockService)
                .step(step)
                .build();

        StepVerifier.create(saga.execute(input))
                .expectErrorMessage("step failed")
                .verify();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.FAILED)).hasSize(1);
    }

    @Test
    void stepContext_resourceAlreadyLocked_propagatesException() {
        ResourceId resourceId = new ResourceId(resourceUuid);
        AlreadyLockedRepository lockedRepo = new AlreadyLockedRepository(resourceUuid.toString());
        SagaLockService lockedService = SagaLockService.create(lockedRepo);

        SagaStep<Input, String, Void> step = new SagaStep<>() {
            @Override public String name() { return "step"; }
            @Override public Class<Input> inputType() { return Input.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(Input in) { throw new UnsupportedOperationException(); }

            @Override
            public Mono<StepResult<String, Void>> execute(Input in, SagaStepContext ctx) {
                return ctx.acquireLock(new ResourceLock(resourceId))
                        .thenReturn(StepResult.stateless("done"));
            }
        };

        Saga<Input, String> saga = Saga.builder("test-saga", Input.class, String.class)
                .withSagaLockService(lockedService)
                .step(step)
                .build();

        StepVerifier.create(saga.execute(input))
                .expectError(ResourceAlreadyLockedException.class)
                .verify();
    }

    // ── multi-step accumulation ───────────────────────────────────────────────

    @Test
    void stepContext_multipleStepsAcquireLocks_allReleasedAtEnd() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        ResourceId res1 = new ResourceId(id1);
        ResourceId res2 = new ResourceId(id2);

        SagaStep<Input, String, Void> step1 = new SagaStep<>() {
            @Override public String name() { return "step1"; }
            @Override public Class<Input> inputType() { return Input.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(Input in) { throw new UnsupportedOperationException(); }

            @Override
            public Mono<StepResult<String, Void>> execute(Input in, SagaStepContext ctx) {
                return ctx.acquireLock(new ResourceLock(res1))
                        .thenReturn(StepResult.stateless("step1-output"));
            }
        };

        SagaStep<String, String, Void> step2 = new SagaStep<>() {
            @Override public String name() { return "step2"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(String in) { throw new UnsupportedOperationException(); }

            @Override
            public Mono<StepResult<String, Void>> execute(String in, SagaStepContext ctx) {
                return ctx.acquireLock(new ResourceLock(res2))
                        .thenReturn(StepResult.stateless("step2-output"));
            }
        };

        Saga<Input, String> saga = Saga.builder("multi-step-saga", Input.class, String.class)
                .withSagaLockService(lockService)
                .step(step1)
                .step(step2)
                .build();

        StepVerifier.create(saga.execute(input))
                .assertNext(r -> assertThat(r).isEqualTo("step2-output"))
                .verifyComplete();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED)).hasSize(2);
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED))
                .extracting(SagaLock::getResourceId)
                .containsExactlyInAnyOrder(id1.toString(), id2.toString());
    }

    // ── combined with saga-wide lock ──────────────────────────────────────────

    @Test
    void stepContext_combinedWithSagaWideLock_allReleasedOnSuccess() {
        UUID sagaLevelId = UUID.randomUUID();
        UUID stepLevelId = UUID.randomUUID();

        record ComboInput(ResourceId sagaResource) {}

        record ComboLock(ResourceId id) implements LockSpec {
            @Override public List<? extends Lockable> lockableResources() { return List.of(id); }
        }

        SagaStep<ComboInput, String, Void> step = new SagaStep<>() {
            @Override public String name() { return "step"; }
            @Override public Class<ComboInput> inputType() { return ComboInput.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(ComboInput in) { throw new UnsupportedOperationException(); }

            @Override
            public Mono<StepResult<String, Void>> execute(ComboInput in, SagaStepContext ctx) {
                ResourceId stepResource = new ResourceId(stepLevelId);
                return ctx.acquireLock(new ComboLock(stepResource))
                        .thenReturn(StepResult.stateless("done"));
            }
        };

        ComboInput comboInput = new ComboInput(new ResourceId(sagaLevelId));

        Saga<ComboInput, String> saga = Saga.builder("combo-saga", ComboInput.class, String.class)
                .withLock(in -> new ComboLock(in.sagaResource()))
                .withSagaLockService(lockService)
                .step(step)
                .build();

        StepVerifier.create(saga.execute(comboInput))
                .assertNext(r -> assertThat(r).isEqualTo("done"))
                .verifyComplete();

        assertThat(repo.locksWithStatus(SagaLockStatus.ACTIVE)).isEmpty();
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED)).hasSize(2);
        assertThat(repo.locksWithStatus(SagaLockStatus.RELEASED))
                .extracting(SagaLock::getResourceId)
                .containsExactlyInAnyOrder(sagaLevelId.toString(), stepLevelId.toString());
    }

    // ── backward compatibility ────────────────────────────────────────────────

    @Test
    void stepContext_oneArgExecute_backwardCompatible() {
        // Step only overrides execute(I) — context parameter must be ignored gracefully
        SagaStep<Input, String, Void> step = new SagaStep<>() {
            @Override public String name() { return "step"; }
            @Override public Class<Input> inputType() { return Input.class; }
            @Override public Class<String> outputType() { return String.class; }

            @Override
            public Mono<StepResult<String, Void>> execute(Input in) {
                return Mono.just(StepResult.stateless("legacy-" + in.name()));
            }
        };

        Saga<Input, String> saga = Saga.builder("legacy-saga", Input.class, String.class)
                .withSagaLockService(lockService)
                .step(step)
                .build();

        StepVerifier.create(saga.execute(input))
                .assertNext(r -> assertThat(r).isEqualTo("legacy-test"))
                .verifyComplete();

        // No locks acquired — step never called ctx.acquireLock()
        assertThat(repo.store).isEmpty();
    }

    @Test
    void stepContext_noLockService_contextNotInjected_executionSucceeds() {
        // Without a lock service, steps receive null context — default fallback to execute(I) applies
        SagaStep<Input, String, Void> step = new SagaStep<>() {
            @Override public String name() { return "step"; }
            @Override public Class<Input> inputType() { return Input.class; }
            @Override public Class<String> outputType() { return String.class; }

            @Override
            public Mono<StepResult<String, Void>> execute(Input in) {
                return Mono.just(StepResult.stateless("no-lock-" + in.name()));
            }
        };

        Saga<Input, String> saga = Saga.builder("no-lock-saga", Input.class, String.class)
                // no .withSagaLockService(...)
                .step(step)
                .build();

        StepVerifier.create(saga.execute(input))
                .assertNext(r -> assertThat(r).isEqualTo("no-lock-test"))
                .verifyComplete();

        assertThat(repo.store).isEmpty();
    }
}
