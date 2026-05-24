package com.saga;

import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.LockableResourceId;
import com.saga.lock.SagaLockService;
import com.saga.step.SagaStep;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.function.Function;

/**
 * Fluent builder for {@link Saga} instances.
 *
 * <p>Obtain an instance via {@link Saga#builder(String, Class, Class)}.
 *
 * @param <I> initial payload type
 * @param <O> final output type
 */
public interface SagaBuilder<I, O> {

    SagaBuilder<I, O> withObserver(SagaLifecycleObserver observer);

    /** Configures exclusive resource locking around every {@link Saga#execute} call. */
    SagaBuilder<I, O> withLock(Function<I, List<LockableResourceId>> resourcesExtractor);

    /** Sets the {@link SagaLockService}. Normally injected by a factory — not for direct use. */
    SagaBuilder<I, O> withSagaLockService(@Nullable SagaLockService sagaLockService);

    /**
     * Extracts a stable business identifier from the initial input.
     * Used by persistence hooks to identify and resume an interrupted execution.
     * If not set, a random ID is generated per execution (no resume support).
     */
    SagaBuilder<I, O> withCorrelationId(Function<I, String> extractor);

    /**
     * Attaches a persistence hook that checkpoints step outputs and supports crash recovery.
     *
     * @see SagaPersistenceHook
     */
    SagaBuilder<I, O> withPersistenceHook(SagaPersistenceHook hook);

    <NI, NO, L> SagaBuilder<I, O> step(SagaStep<NI, NO, L> step);

    SagaBuilder<I, O> parallel(String groupName, SagaStep<?, ?, ?>... subSteps);

    SagaBuilder<I, O> parallel(String groupName, List<SagaStep<?, ?, ?>> subSteps);

    SagaBuilder<I, O> injectProperties(Object properties);

    Saga<I, O> build();

    /**
     * Builds the saga using the given engine's execution strategy and shared configuration.
     * Equivalent to {@code engine.build(this)}.
     */
    default Saga<I, O> build(SagaEngine engine) {
        return engine.build(this);
    }

}
