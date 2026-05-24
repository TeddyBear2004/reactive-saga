package hamburg.engelmann.saga;

import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class SagaParallelTest {

    record AResult(String a) {}
    record BResult(String b) {}

    interface CombinedResult {
        String a();
        String b();
    }

    static SagaStep<Void, AResult, Void> aStep() {
        return new SagaStep<>() {
            @Override public String name() { return "a"; }
            @Override public Class<Void> inputType() { return Void.class; }
            @Override public Class<AResult> outputType() { return AResult.class; }
            @Override public Mono<StepResult<AResult, Void>> execute(Void input) {
                return Mono.just(StepResult.stateless(new AResult("a-done")));
            }
        };
    }

    static SagaStep<Void, BResult, Void> bStep() {
        return new SagaStep<>() {
            @Override public String name() { return "b"; }
            @Override public Class<Void> inputType() { return Void.class; }
            @Override public Class<BResult> outputType() { return BResult.class; }
            @Override public Mono<StepResult<BResult, Void>> execute(Void input) {
                return Mono.just(StepResult.stateless(new BResult("b-done")));
            }
        };
    }

    static SagaStep<Void, BResult, Void> failingBStep() {
        return new SagaStep<>() {
            @Override public String name() { return "b"; }
            @Override public Class<Void> inputType() { return Void.class; }
            @Override public Class<BResult> outputType() { return BResult.class; }
            @Override public Mono<StepResult<BResult, Void>> execute(Void input) {
                return Mono.error(new RuntimeException("b failed"));
            }
        };
    }

    @Test
    void parallel_allStepsComplete() {
        Saga<Void, CombinedResult> saga = Saga.builder("parallel-saga", Void.class, CombinedResult.class)
                .parallel("group", aStep(), bStep())
                .build();

        StepVerifier.create(saga.execute(null))
                .assertNext(r -> {
                    assertThat(r.a()).isEqualTo("a-done");
                    assertThat(r.b()).isEqualTo("b-done");
                })
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void parallel_oneStepFails_sagaFails() {
        Saga<Void, CombinedResult> saga = Saga.builder("parallel-fail-saga", Void.class, CombinedResult.class)
                .parallel("group", aStep(), failingBStep())
                .build();

        StepVerifier.create(saga.execute(null))
                .expectError()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void parallel_stepsFollowedBySequentialStep() {
        SagaStep<CombinedResult, String, Void> mergeStep = new SagaStep<>() {
            @Override public String name() { return "merge"; }
            @Override public Class<CombinedResult> inputType() { return CombinedResult.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(CombinedResult input) {
                return Mono.just(StepResult.stateless(input.a() + "+" + input.b()));
            }
        };

        Saga<Void, String> saga = Saga.builder("mixed-saga", Void.class, String.class)
                .parallel("group", aStep(), bStep())
                .step(mergeStep)
                .build();

        StepVerifier.create(saga.execute(null))
                .assertNext(r -> assertThat(r).isEqualTo("a-done+b-done"))
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }
}
