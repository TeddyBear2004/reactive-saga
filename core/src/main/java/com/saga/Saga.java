package com.saga;

import com.saga.lifecycle.SagaLifecycleObserver;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * A compiled, executable saga flow.
 *
 * <p>Create instances via the fluent {@link SagaBuilder}:
 * <pre>{@code
 * Saga<Input, Output> saga = Saga.builder("MyFlow", Input.class, Output.class)
 *         .step(new ValidateStep())
 *         .step(new ProcessStep())
 *         .build();
 * }</pre>
 *
 * @param <I> initial payload type
 * @param <O> final output type
 */
public interface Saga<I, O> {

    /** Executes the saga with its built-in observer (if any). */
    Mono<O> execute(I initialPayload);

    /**
     * Executes the saga combining the built-in observer with an additional per-call observer.
     * Useful for SSE streams or per-request audit tracing.
     */
    Mono<O> execute(I initialPayload, SagaLifecycleObserver additionalObserver);

    String getName();

    Class<O> getOutputClass();

    static <I, O> SagaBuilder<I, O> builder(String name, Class<I> initClass, Class<O> finalClass) {
        return new SagaBuilder<>(name, List.of(), List.of(initClass), finalClass, null);
    }

}
