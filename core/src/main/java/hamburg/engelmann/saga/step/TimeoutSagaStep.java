package hamburg.engelmann.saga.step;

import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Decorator that applies a per-execution timeout to the wrapped {@link SagaStep}.
 * All other behavior (compensation, name, types) is delegated unchanged.
 *
 * <p>When combined with {@link RetryableSagaStep}, this class should be the <em>inner</em>
 * decorator so the timeout applies to each individual attempt, not to the whole retry sequence.
 *
 * @param <I> input type
 * @param <O> output type
 * @param <L> local state type
 */
public final class TimeoutSagaStep<I, O, L> implements SagaStep<I, O, L> {

    private final SagaStep<I, O, L> delegate;
    private final Duration timeout;

    public TimeoutSagaStep(SagaStep<I, O, L> delegate, Duration timeout) {
        this.delegate = delegate;
        this.timeout = timeout;
    }

    public SagaStep<I, O, L> delegate() { return delegate; }

    public Duration timeout() { return timeout; }

    @Override public String name()            { return delegate.name(); }
    @Override public Class<I> inputType()     { return delegate.inputType(); }
    @Override public Class<O> outputType()    { return delegate.outputType(); }

    @Override
    public Mono<StepResult<O, L>> execute(I input) {
        return delegate.execute(input).timeout(timeout);
    }

    @Override
    public Mono<Void> compensate(L localState) {
        return delegate.compensate(localState);
    }
}
