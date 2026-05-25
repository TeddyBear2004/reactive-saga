package hamburg.engelmann.saga;

import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Verifies that {@link SagaBuilder#step} eagerly detects unmappable and ambiguous inputs
 * via the {@code SagaInputMapper} at build time.
 */
class SagaInputMapperEdgeCaseTest {

    // ── domain types ──────────────────────────────────────────────────────────

    record OrderInput(UUID orderId, String name) {}

    record AmountInput(int amount) {}

    record AmbiguousInput(UUID orderId) {}

    record Step1Output(UUID orderId, String status) {}

    // ── helpers ───────────────────────────────────────────────────────────────

    static <I> SagaStep<I, String, Void> stepReturning(Class<I> inputType, String value) {
        return new SagaStep<>() {
            @Override public String name() { return "step"; }
            @Override public Class<I> inputType() { return inputType; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, Void>> execute(I input) {
                return Mono.just(StepResult.stateless(value));
            }
        };
    }

    static SagaStep<OrderInput, Step1Output, Void> step1() {
        return new SagaStep<>() {
            @Override public String name() { return "step1"; }
            @Override public Class<OrderInput> inputType() { return OrderInput.class; }
            @Override public Class<Step1Output> outputType() { return Step1Output.class; }
            @Override public Mono<StepResult<Step1Output, Void>> execute(OrderInput input) {
                return Mono.just(StepResult.stateless(new Step1Output(input.orderId(), "ok")));
            }
        };
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void step_inputRequiresFieldNotAvailableInHistory_throwsIllegalStateException() {
        // OrderInput only has 'orderId' and 'name'; AmountInput requires 'amount' (int)
        assertThatIllegalStateException()
                .isThrownBy(() ->
                        Saga.builder("saga", OrderInput.class, String.class)
                                .step(stepReturning(AmountInput.class, "x")))
                .withMessageContaining("Cannot find source for")
                .withMessageContaining("amount");
    }

    @Test
    void step_inputRequiresAmbiguousField_throwsIllegalStateException() {
        // After step1, history contains both OrderInput and Step1Output — both have 'orderId'
        // AmbiguousInput(UUID orderId) maps to two sources → ambiguous
        assertThatIllegalStateException()
                .isThrownBy(() ->
                        Saga.builder("saga", OrderInput.class, String.class)
                                .step(step1())    // history: [OrderInput, Step1Output]
                                .step(stepReturning(AmbiguousInput.class, "x")))
                .withMessageContaining("Ambiguous mapping for")
                .withMessageContaining("orderId");
    }
}
