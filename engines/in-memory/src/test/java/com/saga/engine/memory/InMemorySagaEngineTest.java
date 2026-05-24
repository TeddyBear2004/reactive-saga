package com.saga.engine.memory;

import com.saga.Saga;
import com.saga.SagaEngine;
import com.saga.lifecycle.CapturingSagaLifecycleObserver;
import com.saga.step.SagaStep;
import com.saga.step.StepResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class InMemorySagaEngineTest {

    record Out(String value) {}

    static SagaStep<String, Out, Void> simpleStep() {
        return new SagaStep<>() {
            @Override public String name() { return "simple"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(String input) {
                return Mono.just(StepResult.stateless(new Out(input)));
            }
        };
    }

    @Test
    void create_buildsSagaSuccessfully() {
        InMemorySagaEngine engine = InMemorySagaEngine.create();

        Saga<String, Out> saga = engine.build(
                Saga.builder("engine-saga", String.class, Out.class)
                        .step(simpleStep()));

        StepVerifier.create(saga.execute("hello"))
                .assertNext(r -> assertThat(r.value()).isEqualTo("hello"))
                .verifyComplete();
    }

    @Test
    void withObserver_injectsObserverIntoBuiltSaga() {
        CapturingSagaLifecycleObserver observer = CapturingSagaLifecycleObserver.create();
        SagaEngine engine = InMemorySagaEngine.create().withObserver(observer);

        Saga<String, Out> saga = engine.build(
                Saga.builder("obs-engine-saga", String.class, Out.class)
                        .step(simpleStep()));

        saga.execute("x").block();

        assertThat(observer.getCompletedSagas()).containsExactly("obs-engine-saga");
    }

    @Test
    void withObserver_doesNotMutateOriginalEngine() {
        CapturingSagaLifecycleObserver observer = CapturingSagaLifecycleObserver.create();
        InMemorySagaEngine original = InMemorySagaEngine.create();
        InMemorySagaEngine withObs = (InMemorySagaEngine) original.withObserver(observer);

        assertThat(withObs).isNotSameAs(original);

        // Original engine builds a saga without the observer — no events captured
        Saga<String, Out> sagaFromOriginal = original.build(
                Saga.builder("no-obs-saga", String.class, Out.class).step(simpleStep()));

        sagaFromOriginal.execute("x").block();

        assertThat(observer.getCompletedSagas()).isEmpty();
    }
}
