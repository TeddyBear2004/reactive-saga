package com.saga;

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
     * Compensate (roll back) this step. Default: no-op.
     *
     * @param localState state produced during {@link #execute}
     */
    default Mono<Void> compensate(L localState) {
        return Mono.empty();
    }

}
