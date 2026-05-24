package hamburg.engelmann.saga;

import hamburg.engelmann.saga.lifecycle.CapturingSagaLifecycleObserver;
import hamburg.engelmann.saga.step.SagaCompensationException;
import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SagaCompensationTest {

    record Output(String value) {}

    static SagaStep<String, Output, String> trackedStep(List<String> order) {
        return new SagaStep<>() {
            @Override public String name() { return "step1"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Output> outputType() { return Output.class; }
            @Override public Mono<StepResult<Output, String>> execute(String input) {
                return Mono.just(StepResult.of(new Output("step1" + "-output"), "step1"));
            }
            @Override public Mono<Void> compensate(String state) {
                order.add("compensate:" + state);
                return Mono.empty();
            }
        };
    }

    static SagaStep<Output, Output, String> trackedOutputStep(String name, List<String> order) {
        return new SagaStep<>() {
            @Override public String name() { return name; }
            @Override public Class<Output> inputType() { return Output.class; }
            @Override public Class<Output> outputType() { return Output.class; }
            @Override public Mono<StepResult<Output, String>> execute(Output input) {
                return Mono.just(StepResult.of(new Output(name + "-output"), name));
            }
            @Override public Mono<Void> compensate(String state) {
                order.add("compensate:" + state);
                return Mono.empty();
            }
        };
    }

    static SagaStep<Output, Output, Void> failingStep() {
        return new SagaStep<>() {
            @Override public String name() { return "fail"; }
            @Override public Class<Output> inputType() { return Output.class; }
            @Override public Class<Output> outputType() { return Output.class; }
            @Override public Mono<StepResult<Output, Void>> execute(Output input) {
                return Mono.error(new RuntimeException("intentional failure"));
            }
        };
    }

    @Test
    void compensation_runsInReverseOrder() {
        List<String> order = new ArrayList<>();

        Saga<String, Output> saga = Saga.builder("reverse-saga", String.class, Output.class)
                .step(trackedStep(order))
                .step(trackedOutputStep("step2", order))
                .step(failingStep())
                .build();

        StepVerifier.create(saga.execute("input"))
                .expectError()
                .verify();

        // step2 completes before step3 fails → both compensated in reverse
        assertThat(order).containsExactly("compensate:step2", "compensate:step1");
    }

    @Test
    void compensation_onlyCompensatesCompletedSteps() {
        List<String> order = new ArrayList<>();

        // step1 succeeds, step2 fails immediately → only step1 gets compensated
        SagaStep<Output, Output, Void> earlyFail = new SagaStep<>() {
            @Override public String name() { return "early-fail"; }
            @Override public Class<Output> inputType() { return Output.class; }
            @Override public Class<Output> outputType() { return Output.class; }
            @Override public Mono<StepResult<Output, Void>> execute(Output input) {
                return Mono.error(new RuntimeException("early failure"));
            }
        };

        Saga<String, Output> saga = Saga.builder("early-fail-saga", String.class, Output.class)
                .step(trackedStep(order))
                .step(earlyFail)
                .step(trackedOutputStep("step3", order))
                .build();

        StepVerifier.create(saga.execute("input"))
                .expectError()
                .verify();

        assertThat(order).containsExactly("compensate:step1");
    }

    @Test
    void compensationFailure_wrappedInSagaCompensationException() {
        SagaStep<String, Output, String> badCompensationStep = new SagaStep<>() {
            @Override public String name() { return "bad-comp"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Output> outputType() { return Output.class; }
            @Override public Mono<StepResult<Output, String>> execute(String input) {
                return Mono.just(StepResult.of(new Output("out"), "state"));
            }
            @Override public Mono<Void> compensate(String state) {
                return Mono.error(new RuntimeException("compensation failed!"));
            }
        };

        Saga<String, Output> saga = Saga.builder("bad-comp-saga", String.class, Output.class)
                .step(badCompensationStep)
                .step(failingStep())
                .build();

        StepVerifier.create(saga.execute("input"))
                .expectError(SagaCompensationException.class)
                .verify();
    }

    @Test
    void compensationFailure_containsOriginalError() {
        SagaStep<String, Output, String> badCompStep = new SagaStep<>() {
            @Override public String name() { return "bad"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Output> outputType() { return Output.class; }
            @Override public Mono<StepResult<Output, String>> execute(String input) {
                return Mono.just(StepResult.of(new Output("x"), "s"));
            }
            @Override public Mono<Void> compensate(String state) {
                return Mono.error(new IllegalStateException("comp boom"));
            }
        };

        Saga<String, Output> saga = Saga.builder("original-err-saga", String.class, Output.class)
                .step(badCompStep)
                .step(failingStep())
                .build();

        StepVerifier.create(saga.execute("x"))
                .expectErrorSatisfies(err -> {
                    assertThat(err).isInstanceOf(SagaCompensationException.class);
                    SagaCompensationException ex = (SagaCompensationException) err;
                    assertThat(ex.getOriginalError().getMessage()).isEqualTo("intentional failure");
                    assertThat(ex.getCompensationFailures()).hasSize(1);
                    assertThat(ex.getCompensationFailures().getFirst().stepName()).isEqualTo("bad");
                })
                .verify();
    }

    @Test
    void observer_capturesCompensationEvents() {
        CapturingSagaLifecycleObserver observer = CapturingSagaLifecycleObserver.create();

        Saga<String, Output> saga = Saga.builder("obs-comp-saga", String.class, Output.class)
                .withObserver(observer)
                .step(trackedStep(new ArrayList<>()))
                .step(failingStep())
                .build();

        StepVerifier.create(saga.execute("input"))
                .expectError()
                .verify();

        assertThat(observer.getErrors()).hasSize(1);
        assertThat(observer.getCompensatedSagas()).containsExactly("obs-comp-saga");
    }
}
