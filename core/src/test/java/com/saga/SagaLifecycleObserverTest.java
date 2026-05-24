package com.saga;

import com.saga.lifecycle.CapturingSagaLifecycleObserver;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.step.SagaStep;
import com.saga.step.StepResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SagaLifecycleObserverTest {

    record Out(String value) {}

    static SagaStep<String, Out, Void> successStep(String name) {
        return new SagaStep<>() {
            @Override public String name() { return name; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(String input) {
                return Mono.just(StepResult.stateless(new Out(name)));
            }
        };
    }

    static SagaStep<Out, Out, Void> outStep() {
        return new SagaStep<>() {
            @Override public String name() { return "step-b"; }
            @Override public Class<Out> inputType() { return Out.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(Out input) {
                return Mono.just(StepResult.stateless(new Out("step-b")));
            }
        };
    }

    static SagaStep<Out, Out, Void> failStep() {
        return new SagaStep<>() {
            @Override public String name() { return "s2"; }
            @Override public Class<Out> inputType() { return Out.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(Out input) {
                return Mono.error(new RuntimeException("step failed"));
            }
        };
    }

    // ── CapturingSagaLifecycleObserver ────────────────────────────────────────

    @Test
    void capturing_recordsStartedAndCompletedSaga() {
        CapturingSagaLifecycleObserver obs = CapturingSagaLifecycleObserver.create();

        Saga<String, Out> saga = Saga.builder("my-saga", String.class, Out.class)
                .withObserver(obs)
                .step(successStep("s1"))
                .build();

        saga.execute("x").block();

        assertThat(obs.getStartedSagas()).containsExactly("my-saga");
        assertThat(obs.getCompletedSagas()).containsExactly("my-saga");
        assertThat(obs.hasErrors()).isFalse();
    }

    @Test
    void capturing_recordsStartedAndCompletedSteps() {
        CapturingSagaLifecycleObserver obs = CapturingSagaLifecycleObserver.create();

        Saga<String, Out> saga = Saga.builder("steps-saga", String.class, Out.class)
                .withObserver(obs)
                .step(successStep("step-a"))
                .step(outStep())
                .build();

        saga.execute("x").block();

        assertThat(obs.getStartedSteps())
                .extracting(CapturingSagaLifecycleObserver.StepEvent::stepName)
                .containsExactly("step-a", "step-b");
        assertThat(obs.getCompletedSteps())
                .extracting(CapturingSagaLifecycleObserver.StepEvent::stepName)
                .containsExactly("step-a", "step-b");
    }

    @Test
    void capturing_recordsErrors() {
        CapturingSagaLifecycleObserver obs = CapturingSagaLifecycleObserver.create();

        Saga<String, Out> saga = Saga.builder("err-saga", String.class, Out.class)
                .withObserver(obs)
                .step(successStep("s1"))
                .step(failStep())
                .build();

        StepVerifier.create(saga.execute("x"))
                .expectError()
                .verify();

        assertThat(obs.hasErrors()).isTrue();
        assertThat(obs.getErrors()).hasSize(1);
        assertThat(obs.getErrors().getFirst().sagaName()).isEqualTo("err-saga");
        assertThat(obs.getFailedSteps())
                .extracting(CapturingSagaLifecycleObserver.StepErrorEvent::stepName)
                .containsExactly("s2");
    }

    @Test
    void capturing_reset_clearsAllState() {
        CapturingSagaLifecycleObserver obs = CapturingSagaLifecycleObserver.create();

        Saga<String, Out> saga = Saga.builder("reset-saga", String.class, Out.class)
                .withObserver(obs)
                .step(successStep("s1"))
                .build();

        saga.execute("x").block();
        assertThat(obs.getStartedSagas()).isNotEmpty();

        obs.reset();

        assertThat(obs.getStartedSagas()).isEmpty();
        assertThat(obs.getStartedSteps()).isEmpty();
        assertThat(obs.getCompletedSteps()).isEmpty();
        assertThat(obs.getCompletedSagas()).isEmpty();
        assertThat(obs.getErrors()).isEmpty();
        assertThat(obs.getCompensatedSagas()).isEmpty();
        assertThat(obs.hasErrors()).isFalse();
    }

    // ── per-execution observer ────────────────────────────────────────────────

    @Test
    void additionalObserver_combinedWithBuiltIn() {
        CapturingSagaLifecycleObserver builtIn = CapturingSagaLifecycleObserver.create();
        CapturingSagaLifecycleObserver perCall = CapturingSagaLifecycleObserver.create();

        Saga<String, Out> saga = Saga.builder("combined-saga", String.class, Out.class)
                .withObserver(builtIn)
                .step(successStep("s1"))
                .build();

        saga.execute("x", perCall).block();

        assertThat(builtIn.getCompletedSagas()).containsExactly("combined-saga");
        assertThat(perCall.getCompletedSagas()).containsExactly("combined-saga");
    }

    @Test
    void additionalObserver_withoutBuiltIn_works() {
        CapturingSagaLifecycleObserver perCall = CapturingSagaLifecycleObserver.create();

        Saga<String, Out> saga = Saga.builder("no-builtin-saga", String.class, Out.class)
                .step(successStep("s1"))
                .build();

        saga.execute("x", perCall).block();

        assertThat(perCall.getCompletedSagas()).containsExactly("no-builtin-saga");
    }

    // ── SagaLifecycleObserver.combine ─────────────────────────────────────────

    @Test
    void combine_delegatesToAllObservers() {
        List<String> events1 = new ArrayList<>();
        List<String> events2 = new ArrayList<>();

        SagaLifecycleObserver o1 = new SagaLifecycleObserver() {
            @Override public void onSagaCompleted(String name) { events1.add("completed:" + name); }
        };
        SagaLifecycleObserver o2 = new SagaLifecycleObserver() {
            @Override public void onSagaCompleted(String name) { events2.add("completed:" + name); }
        };

        Saga<String, Out> saga = Saga.builder("multi-obs-saga", String.class, Out.class)
                .withObserver(SagaLifecycleObserver.combine(o1, o2))
                .step(successStep("s1"))
                .build();

        saga.execute("x").block();

        assertThat(events1).containsExactly("completed:multi-obs-saga");
        assertThat(events2).containsExactly("completed:multi-obs-saga");
    }
}
