package hamburg.engelmann.saga.step;

import hamburg.engelmann.saga.lock.SagaStepContext;
import reactor.core.publisher.Mono;

/**
 * A single step within a Saga.
 *
 * @param <I> input type (output of the previous step)
 * @param <O> output type (passed to the next step)
 * @param <L> local state type stored for compensation
 */
public interface SagaStep<I, O, L> {

    /** Human-readable name used in logging. */
    String name();

    /** Expected input class; used for runtime type validation. */
    Class<I> inputType();

    /** Output class; used to build the proxy/record input for the next step. */
    Class<O> outputType();

    /**
     * Execute the forward action.
     *
     * @param input output of the previous step (or the initial payload)
     * @return Mono emitting the step result
     */
    Mono<StepResult<O, L>> execute(I input);

    /**
     * Execute the forward action with access to the saga's lock context.
     *
     * <p>Override this method when the resource ID to lock is only known at runtime
     * (e.g. after an async repository call). Call {@link SagaStepContext#acquireLock}
     * to acquire a lock and hand it over to the saga — it will be held until saga end.
     *
     * <p>By default delegates to {@link #execute(Object)}.
     *
     * @param input   output of the previous step (or the initial payload)
     * @param context provides {@link SagaStepContext#acquireLock} for runtime locking
     */
    default Mono<StepResult<O, L>> execute(I input, SagaStepContext context) {
        return execute(input);
    }

    /**
     * Compensate (roll back) this step. Default: no-op.
     *
     * @param localState state produced during {@link #execute}
     */
    default Mono<Void> compensate(L localState) {
        return Mono.empty();
    }

}
