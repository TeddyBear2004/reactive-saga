package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.di.internal.InputResolver;
import hamburg.engelmann.saga.lock.SagaStepContext;
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

    String stepName() { return step.name(); }

    @SuppressWarnings("unchecked")
    Mono<StepResult<Object, Object>> executeValidated(Object rawInput, List<Object> executionOutputs,
                                                       @Nullable SagaStepContext stepContext) {
        Object actualInput = inputResolver.resolve(rawInput, executionOutputs);
        Class<I> expectedType = step.inputType();

        if (actualInput != null && !expectedType.isInstance(actualInput)) {
            return Mono.error(new IllegalArgumentException(
                    String.format("[Saga Transition Error] Step '%s' expects '%s' but got '%s'",
                                  step.name(), expectedType.getSimpleName(), actualInput.getClass().getSimpleName())
            ));
        }

        I castedInput = expectedType.cast(actualInput);

        // Wrap in Mono.defer so that retries re-invoke step.execute() (not just re-subscribe
        // to the same Mono instance). This matches the behavior of the old RetryableSagaStep.
        Mono<StepResult<O, L>> stepMono = Mono.defer(() -> (stepContext != null)
                ? step.execute(castedInput, stepContext)
                : step.execute(castedInput));
        if (timeout != null)    stepMono = stepMono.timeout(timeout);
        if (retrySpec != null)  stepMono = stepMono.retryWhen(retrySpec);

        return ((Mono<StepResult<Object, Object>>) (Mono<?>) stepMono)
                .map(res -> new StepResult<>(res.output(), res.localState()));
    }

    @SuppressWarnings("unchecked")
    Mono<Void> compensateValidated(Object localState) {
        return step.compensate((L) localState);
    }
}
