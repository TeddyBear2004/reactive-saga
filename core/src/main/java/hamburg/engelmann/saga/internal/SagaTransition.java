package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;

public class SagaTransition<I, O, L> {

    private final SagaStep<I, O, L> step;
    private final InputResolver<I> inputResolver;
    private final @Nullable Duration timeout;
    private final @Nullable Retry retrySpec;

    public SagaTransition(SagaStep<I, O, L> step, InputResolver<I> inputResolver) {
        this(step, inputResolver, null, null);
    }

    public SagaTransition(SagaStep<I, O, L> step, InputResolver<I> inputResolver,
                          @Nullable Duration timeout) {
        this(step, inputResolver, timeout, null);
    }

    public SagaTransition(SagaStep<I, O, L> step, InputResolver<I> inputResolver,
                          @Nullable Duration timeout, @Nullable Retry retrySpec) {
        this.step = step;
        this.inputResolver = inputResolver;
        this.timeout = timeout;
        this.retrySpec = retrySpec;
    }

    SagaTransition<I, O, L> withTimeout(Duration t) {
        return new SagaTransition<>(step, inputResolver, t, retrySpec);
    }

    SagaTransition<I, O, L> withRetry(Retry spec) {
        return new SagaTransition<>(step, inputResolver, timeout, spec);
    }

    @Nullable Duration timeout() { return timeout; }

    @Nullable Retry retrySpec() { return retrySpec; }

    String stepName() { return step.name(); }

    Mono<StepResult<Object, Object>> executeValidated(Object rawInput, List<Object> executionOutputs) {
        Object actualInput = inputResolver.resolve(rawInput, executionOutputs);
        Class<I> expectedType = step.inputType();

        if (actualInput != null && !expectedType.isInstance(actualInput)) {
            return Mono.error(new IllegalArgumentException(
                    String.format("[Saga Transition Error] Step '%s' expects '%s' but got '%s'",
                                  step.name(), expectedType.getSimpleName(), actualInput.getClass().getSimpleName())
            ));
        }

        return step.execute(expectedType.cast(actualInput))
                .map(res -> new StepResult<>(res.output(), res.localState()));
    }

    @SuppressWarnings("unchecked")
    Mono<Void> compensateValidated(Object localState) {
        return step.compensate((L) localState);
    }
}
