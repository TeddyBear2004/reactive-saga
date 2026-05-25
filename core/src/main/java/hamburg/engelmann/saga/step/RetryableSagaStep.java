package hamburg.engelmann.saga.step;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Decorator that retries the wrapped {@link SagaStep} according to a {@link Retry} spec.
 * All other behavior (compensation, name, types) is delegated unchanged.
 *
 * <p>{@code execute} wraps the delegate call in {@link Mono#defer} so that each retry
 * re-invokes {@code delegate.execute(input)} from scratch — including any side effects
 * that occur eagerly inside the delegate's {@code execute} implementation.
 *
 * <p>When combined with {@link TimeoutSagaStep}, this class should be the <em>outer</em>
 * decorator so the retry wraps each individually-timed attempt.
 *
 * @param <I> input type
 * @param <O> output type
 * @param <L> local state type
 */
public final class RetryableSagaStep<I, O, L> implements SagaStep<I, O, L> {

    private final SagaStep<I, O, L> delegate;
    private final Retry retrySpec;

    public RetryableSagaStep(SagaStep<I, O, L> delegate, Retry retrySpec) {
        this.delegate = delegate;
        this.retrySpec = retrySpec;
    }

    public SagaStep<I, O, L> delegate() { return delegate; }

    public Retry retrySpec() { return retrySpec; }

    @Override public String name()            { return delegate.name(); }
    @Override public Class<I> inputType()     { return delegate.inputType(); }
    @Override public Class<O> outputType()    { return delegate.outputType(); }

    @Override
    public Mono<StepResult<O, L>> execute(I input) {
        return Mono.defer(() -> delegate.execute(input)).retryWhen(retrySpec);
    }

    @Override
    public Mono<Void> compensate(L localState) {
        return delegate.compensate(localState);
    }
}
