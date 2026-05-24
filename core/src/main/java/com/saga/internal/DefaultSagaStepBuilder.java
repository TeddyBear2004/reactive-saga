package com.saga.internal;

import com.saga.Saga;
import com.saga.SagaBuilder;
import com.saga.SagaEngine;
import com.saga.SagaPersistenceHook;
import com.saga.SagaStepBuilder;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.LockableResourceId;
import com.saga.lock.SagaLockService;
import com.saga.step.SagaStep;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * Package-private wrapper returned by {@link DefaultSagaBuilder#step} and
 * {@link DefaultSagaBuilder#parallel}. Delegates all {@link SagaBuilder} methods to the
 * underlying builder. Overrides {@link #timeout} to apply a timeout to the most recently
 * added step/group transition.
 */
class DefaultSagaStepBuilder<I, O> implements SagaStepBuilder<I, O> {

    private final DefaultSagaBuilder<I, O> inner;

    DefaultSagaStepBuilder(DefaultSagaBuilder<I, O> inner) {
        this.inner = inner;
    }

    /**
     * Sets a timeout for the most recently added step.
     * Delegates to {@link DefaultSagaBuilder#withLastTransitionTimeout}.
     */
    @Override
    public SagaBuilder<I, O> timeout(Duration stepTimeout) {
        return inner.withLastTransitionTimeout(stepTimeout);
    }

    // All SagaBuilder methods delegate to inner:

    @Override
    public SagaBuilder<I, O> withObserver(SagaLifecycleObserver observer) {
        return inner.withObserver(observer);
    }

    @Override
    public SagaBuilder<I, O> withLock(Function<I, List<LockableResourceId>> resourcesExtractor) {
        return inner.withLock(resourcesExtractor);
    }

    @Override
    public SagaBuilder<I, O> withSagaLockService(@Nullable SagaLockService sagaLockService) {
        return inner.withSagaLockService(sagaLockService);
    }

    @Override
    public SagaBuilder<I, O> withCorrelationId(Function<I, String> extractor) {
        return inner.withCorrelationId(extractor);
    }

    @Override
    public SagaBuilder<I, O> withPersistenceHook(SagaPersistenceHook hook) {
        return inner.withPersistenceHook(hook);
    }

    @Override
    public <NI, NO, L> SagaStepBuilder<I, O> step(SagaStep<NI, NO, L> step) {
        return inner.step(step);
    }

    @Override
    public SagaStepBuilder<I, O> parallel(String groupName, SagaStep<?, ?, ?>... subSteps) {
        return inner.parallel(groupName, subSteps);
    }

    @Override
    public SagaStepBuilder<I, O> parallel(String groupName, List<SagaStep<?, ?, ?>> subSteps) {
        return inner.parallel(groupName, subSteps);
    }

    @Override
    public SagaBuilder<I, O> injectProperties(Object properties) {
        return inner.injectProperties(properties);
    }

    @Override
    public Saga<I, O> build() {
        return inner.build();
    }

    @Override
    public Saga<I, O> build(SagaEngine engine) {
        return inner.build(engine);
    }
}
