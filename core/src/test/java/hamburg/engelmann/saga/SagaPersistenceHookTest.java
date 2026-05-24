package hamburg.engelmann.saga;

import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class SagaPersistenceHookTest {

    record Out(String value) {}

    // ── tracking hook ─────────────────────────────────────────────────────────

    static class TrackingHook implements SagaPersistenceHook {
        final List<String> afterStepCalls = new CopyOnWriteArrayList<>();
        boolean completedCalled = false;
        boolean failedCalled = false;

        @Override
        public Mono<Void> afterStep(String sagaName, String correlationId,
                                    int transitionIndex, List<Object> stepOutputs) {
            afterStepCalls.add(sagaName + ":" + correlationId + ":" + transitionIndex);
            return Mono.empty();
        }

        @Override
        public Mono<SagaResumePoint> loadResumePoint(String sagaName, String correlationId) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> onCompleted(String sagaName, String correlationId) {
            completedCalled = true;
            return Mono.empty();
        }

        @Override
        public Mono<Void> onFailed(String sagaName, String correlationId) {
            failedCalled = true;
            return Mono.empty();
        }
    }

    TrackingHook hook;

    @BeforeEach
    void setUp() {
        hook = new TrackingHook();
    }

    static SagaStep<String, Out, Void> successStep() {
        return new SagaStep<>() {
            @Override public String name() { return "s1"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(String input) {
                return Mono.just(StepResult.stateless(new Out("s1")));
            }
        };
    }

    static SagaStep<Out, Out, Void> chainStep() {
        return new SagaStep<>() {
            @Override public String name() { return "s2"; }
            @Override public Class<Out> inputType() { return Out.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(Out input) {
                return Mono.just(StepResult.stateless(new Out("s2")));
            }
        };
    }

    static SagaStep<Out, Out, Void> failStep() {
        return new SagaStep<>() {
            @Override public String name() { return "fail"; }
            @Override public Class<Out> inputType() { return Out.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(Out input) {
                return Mono.error(new RuntimeException("boom"));
            }
        };
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void afterStep_calledForEachStep() {
        Saga<String, Out> saga = Saga.builder("persist-saga", String.class, Out.class)
                .withCorrelationId(id -> id)
                .withPersistenceHook(hook)
                .step(successStep())
                .step(chainStep())
                .build();

        saga.execute("cid-123").block();

        assertThat(hook.afterStepCalls).containsExactly(
                "persist-saga:cid-123:0",
                "persist-saga:cid-123:1"
        );
    }

    @Test
    void onCompleted_calledAfterSuccess() {
        Saga<String, Out> saga = Saga.builder("complete-saga", String.class, Out.class)
                .withCorrelationId(id -> id)
                .withPersistenceHook(hook)
                .step(successStep())
                .build();

        saga.execute("cid-abc").block();

        assertThat(hook.completedCalled).isTrue();
        assertThat(hook.failedCalled).isFalse();
    }

    @Test
    void onFailed_calledAfterFailure() {
        Saga<String, Out> saga = Saga.builder("failed-saga", String.class, Out.class)
                .withCorrelationId(id -> id)
                .withPersistenceHook(hook)
                .step(successStep())
                .step(failStep())
                .build();

        StepVerifier.create(saga.execute("cid-fail"))
                .expectError()
                .verify();

        assertThat(hook.failedCalled).isTrue();
        assertThat(hook.completedCalled).isFalse();
    }

    @Test
    void loadResumePoint_skipsAlreadyCompletedSteps() {
        List<String> executed = new ArrayList<>();

        SagaStep<String, Out, Void> trackedStep1 = new SagaStep<>() {
            @Override public String name() { return "t1"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(String input) {
                executed.add("t1");
                return Mono.just(StepResult.stateless(new Out("out1")));
            }
        };
        SagaStep<Out, Out, Void> trackedStep2 = new SagaStep<>() {
            @Override public String name() { return "t2"; }
            @Override public Class<Out> inputType() { return Out.class; }
            @Override public Class<Out> outputType() { return Out.class; }
            @Override public Mono<StepResult<Out, Void>> execute(Out input) {
                executed.add("t2");
                return Mono.just(StepResult.stateless(new Out("out2")));
            }
        };

        // Hook that returns a resume point saying step 0 (t1) was already done
        SagaPersistenceHook resumeHook = new SagaPersistenceHook() {
            @Override
            public Mono<Void> afterStep(String sn, String cid, int idx, List<Object> outputs) {
                return Mono.empty();
            }
            @Override
            public Mono<SagaResumePoint> loadResumePoint(String sagaName, String correlationId) {
                return Mono.just(SagaResumePoint.of(1, List.of(new Out("out1"))));
            }
        };

        Saga<String, Out> saga = Saga.builder("resume-saga", String.class, Out.class)
                .withCorrelationId(id -> id)
                .withPersistenceHook(resumeHook)
                .step(trackedStep1)
                .step(trackedStep2)
                .build();

        saga.execute("cid").block();

        // t1 should be skipped, only t2 executes
        assertThat(executed).containsExactly("t2");
    }
}
