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

    <NI, NO, L> SagaBuilder<I, O> step(SagaStep<NI, NO, L> step);

    SagaBuilder<I, O> parallel(String groupName, SagaStep<?, ?, ?>... subSteps);

    SagaBuilder<I, O> parallel(String groupName, List<SagaStep<?, ?, ?>> subSteps);

    SagaBuilder<I, O> injectProperties(Object properties);

    Saga<I, O> build();

}
