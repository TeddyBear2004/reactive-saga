package hamburg.engelmann.saga;

import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ParallelDebugTest {

    record AResult(String a) {}
    record BResult(String b) {}

    interface CombinedResult {
        String a();
        String b();
    }

    @Test
    void debug_parallel_withBlock() {
        SagaStep<Void, AResult, Void> aStep = new SagaStep<>() {
            @Override public String name() { return "a"; }
            @Override public Class<Void> inputType() { return Void.class; }
            @Override public Class<AResult> outputType() { return AResult.class; }
            @Override public Mono<StepResult<AResult, Void>> execute(Void input) {
                System.out.println("aStep.execute called");
                return Mono.just(StepResult.stateless(new AResult("a-done")));
            }
        };

        SagaStep<Void, BResult, Void> bStep = new SagaStep<>() {
            @Override public String name() { return "b"; }
            @Override public Class<Void> inputType() { return Void.class; }
            @Override public Class<BResult> outputType() { return BResult.class; }
            @Override public Mono<StepResult<BResult, Void>> execute(Void input) {
                System.out.println("bStep.execute called");
                return Mono.just(StepResult.stateless(new BResult("b-done")));
            }
        };

        Saga<Void, CombinedResult> saga = Saga.builder("parallel-saga", Void.class, CombinedResult.class)
                .parallel("group", aStep, bStep)
                .build();

        System.out.println("Calling saga.execute(null)");
        Mono<CombinedResult> mono = saga.execute(null);
        System.out.println("Calling block()");
        CombinedResult result = mono.log().block(Duration.ofSeconds(5));
        System.out.println("block() returned: " + result);
        assertThat(result).isNotNull();
        System.out.println("a=" + result.a() + ", b=" + result.b());
    }
}
