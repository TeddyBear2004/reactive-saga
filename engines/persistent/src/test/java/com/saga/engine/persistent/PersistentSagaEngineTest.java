package com.saga.engine.persistent;

import com.saga.Saga;
import com.saga.lifecycle.CapturingSagaLifecycleObserver;
import com.saga.step.SagaStep;
import com.saga.step.StepResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class PersistentSagaEngineTest {

    // ── in-memory repository stub ─────────────────────────────────────────────

    static class InMemoryRepository implements SagaExecutionRepository {
        final Map<SagaExecutionId, SagaExecutionState> store = new ConcurrentHashMap<>();

        @Override
        public Mono<SagaExecutionState> save(SagaExecutionState state) {
            store.put(state.id(), state);
            return Mono.just(state);
        }

        @Override
        public Mono<SagaExecutionState> findById(SagaExecutionId id) {
            return Mono.justOrEmpty(store.get(id));
        }

        @Override
        public Flux<SagaExecutionState> findAllInProgress() {
            return Flux.fromIterable(store.values())
                    .filter(s -> s.status() == SagaExecutionStatus.IN_PROGRESS);
        }

        @Override
        public Mono<Void> deleteById(SagaExecutionId id) {
            store.remove(id);
            return Mono.empty();
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    record Out(String value) {}

    static SagaStep<String, Out, Void> step(String name) {
        return new SagaStep<>() {
            @Override public String name() { return name; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(String input) {
                return Mono.just(StepResult.stateless(new Out(name + ":" + input)));
            }
        };
    }

    static SagaStep<Out, Out, Void> chainStep(String name) {
        return new SagaStep<>() {
            @Override public String name() { return name; }
            @Override public Class<Out> inputType() { return Out.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(Out input) {
                return Mono.just(StepResult.stateless(new Out(name + ":" + input.value())));
            }
        };
    }

    static SagaStep<Out, Out, Void> failStep() {
        return new SagaStep<>() {
            @Override public String name() { return "fail"; }
            @Override public Class<Out> inputType() { return Out.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(Out input) {
                return Mono.error(new RuntimeException("step failed"));
            }
        };
    }

    InMemoryRepository repository;
    SagaStateSerializer serializer;
    PersistentSagaEngine engine;

    @BeforeEach
    void setUp() {
        repository = new InMemoryRepository();
        serializer = SagaStateSerializer.jacksonWithFqcn(new ObjectMapper());
        engine = new PersistentSagaEngine(repository, serializer);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void happyPath_persistsStateAndCompletesSuccessfully() {
        Saga<String, Out> saga = engine.build(
                Saga.builder("persist-saga", String.class, Out.class)
                        .withCorrelationId(id -> id)
                        .step(step("s1"))
                        .step(chainStep("s2")));

        StepVerifier.create(saga.execute("cid-1"))
                .assertNext(r -> assertThat(r.value()).isEqualTo("s2:s1:cid-1"))
                .verifyComplete();

        SagaExecutionState state = repository.store.get(new SagaExecutionId("persist-saga", "cid-1"));
        assertThat(state).isNotNull();
        assertThat(state.status()).isEqualTo(SagaExecutionStatus.COMPLETED);
        assertThat(state.currentStepIndex()).isEqualTo(2);
    }

    @Test
    void failure_persistsFailedStatus() {
        Saga<String, Out> saga = engine.build(
                Saga.builder("fail-saga", String.class, Out.class)
                        .withCorrelationId(id -> id)
                        .step(step("s1"))
                        .step(failStep()));

        StepVerifier.create(saga.execute("cid-fail"))
                .expectError()
                .verify();

        SagaExecutionState state = repository.store.get(new SagaExecutionId("fail-saga", "cid-fail"));
        assertThat(state).isNotNull();
        assertThat(state.status()).isEqualTo(SagaExecutionStatus.FAILED);
    }

    @Test
    void resume_skipsAlreadyCompletedSteps() {
        List<String> executed = new ArrayList<>();

        SagaStep<String, Out, Void> trackedS1 = new SagaStep<>() {
            @Override public String name() { return "s1"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(String input) {
                executed.add("s1");
                return Mono.just(StepResult.stateless(new Out("s1-out")));
            }
        };
        SagaStep<Out, Out, Void> trackedS2 = new SagaStep<>() {
            @Override public String name() { return "s2"; }
            @Override public Class<Out> inputType() { return Out.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(Out input) {
                executed.add("s2");
                return Mono.just(StepResult.stateless(new Out("s2-out")));
            }
        };

        Saga<String, Out> saga = engine.build(
                Saga.builder("resume-saga", String.class, Out.class)
                        .withCorrelationId(id -> id)
                        .step(trackedS1)
                        .step(trackedS2));

        // First execution — both steps run
        saga.execute("cid-resume").block();
        assertThat(executed).containsExactly("s1", "s2");
        executed.clear();

        // Manually reset the repo state to IN_PROGRESS at step 1 to simulate a crash after s1
        SagaExecutionId id = new SagaExecutionId("resume-saga", "cid-resume");
        SagaExecutionState existing = repository.store.get(id);
        byte[] s1Serialized = serializer.serialize(new Out("s1-out"));
        repository.store.put(id, new SagaExecutionState(
                id, SagaExecutionStatus.IN_PROGRESS, 1,
                List.of(s1Serialized), existing.createdAt(), existing.updatedAt()));

        // Second execution — s1 should be skipped
        saga.execute("cid-resume").block();
        assertThat(executed).containsExactly("s2");
    }

    @Test
    void withObserver_injectsObserver() {
        CapturingSagaLifecycleObserver observer = CapturingSagaLifecycleObserver.create();
        PersistentSagaEngine engineWithObs = (PersistentSagaEngine) engine.withObserver(observer);

        Saga<String, Out> saga = engineWithObs.build(
                Saga.builder("obs-persist-saga", String.class, Out.class)
                        .withCorrelationId(id -> id)
                        .step(step("s1")));

        saga.execute("cid-obs").block();

        assertThat(observer.getCompletedSagas()).containsExactly("obs-persist-saga");
    }
}
