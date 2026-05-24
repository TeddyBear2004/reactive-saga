package com.saga;

import com.saga.lock.LockableResourceId;
import com.saga.lock.SagaLockContext;
import com.saga.lock.SagaLockService;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;

/**
 * A compiled, executable saga flow.
 *
 * <p>Create instances via the fluent builder:
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
public class Saga<I, O> {

    private final @Nullable SagaLifecycleObserver observer;
    private final SagaExecution<I, O> execution;
    private final @Nullable Function<I, List<LockableResourceId>> lockResourcesExtractor;
    private final @Nullable SagaLockService sagaLockService;

    Saga(String name,
         List<SagaTransition<?, ?, ?>> transitions,
         Class<O> outputClass,
         @Nullable SagaLifecycleObserver observer,
         InputResolver<O> finalOutputResolver,
         @Nullable Function<I, List<LockableResourceId>> lockResourcesExtractor,
         @Nullable SagaLockService sagaLockService) {
        this.observer = observer;
        this.execution = new SagaExecution<>(name, transitions, finalOutputResolver, outputClass);
        this.lockResourcesExtractor = lockResourcesExtractor;
        this.sagaLockService = sagaLockService;
    }

    Saga(@Nullable SagaLifecycleObserver observer, SagaExecution<I, O> execution) {
        this.observer = observer;
        this.execution = execution;
        this.lockResourcesExtractor = null;
        this.sagaLockService = null;
    }

    public static <I, O> SagaBuilder<I, O> builder(String name, Class<I> initClass, Class<O> finalClass) {
        return new SagaBuilder<>(name, List.of(), List.of(initClass), finalClass, null);
    }

    /** Executes the saga with its built-in observer (if any). */
    public Mono<O> execute(I initialPayload) {
        return wrapWithLock(initialPayload, execution.execute(initialPayload, this.observer));
    }

    /**
     * Executes the saga combining the built-in observer with an additional per-call observer.
     * Useful for SSE streams or per-request audit tracing.
     */
    public Mono<O> execute(I initialPayload, SagaLifecycleObserver additionalObserver) {
        SagaLifecycleObserver combined = (this.observer != null)
                ? SagaLifecycleObserver.combine(this.observer, additionalObserver)
                : additionalObserver;
        return wrapWithLock(initialPayload, execution.execute(initialPayload, combined));
    }

    private Mono<O> wrapWithLock(I input, Mono<O> mono) {
        if (sagaLockService == null || lockResourcesExtractor == null) return mono;
        return sagaLockService.withLock(SagaLockContext.forSaga(execution.getName()),
                                         lockResourcesExtractor.apply(input), mono);
    }

    public String getName() { return execution.getName(); }
    public Class<O> getOutputClass() { return execution.getOutputClass(); }

}
