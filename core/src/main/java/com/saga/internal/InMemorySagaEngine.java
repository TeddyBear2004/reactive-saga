package com.saga.internal;

import com.saga.Saga;
import com.saga.SagaBuilder;
import com.saga.SagaEngine;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.SagaLockService;
import org.jspecify.annotations.Nullable;

/**
 * Stateless, in-memory {@link SagaEngine} implementation.
 * Execution state lives entirely in the reactive chain — no persistence across calls.
 *
 * <p>Obtain via {@link SagaEngine#inMemory()}.
 */
public final class InMemorySagaEngine implements SagaEngine {

    private final @Nullable SagaLifecycleObserver observer;
    private final @Nullable SagaLockService lockService;

    public InMemorySagaEngine(@Nullable SagaLifecycleObserver observer,
                               @Nullable SagaLockService lockService) {
        this.observer = observer;
        this.lockService = lockService;
    }

    @Override
    public SagaEngine withObserver(SagaLifecycleObserver observer) {
        return new InMemorySagaEngine(observer, this.lockService);
    }

    @Override
    public SagaEngine withLockService(SagaLockService lockService) {
        return new InMemorySagaEngine(this.observer, lockService);
    }

    @Override
    public <I, O> Saga<I, O> build(SagaBuilder<I, O> builder) {
        SagaBuilder<I, O> configured = builder;
        if (observer != null) configured = configured.withObserver(observer);
        if (lockService != null) configured = configured.withSagaLockService(lockService);
        return configured.build();
    }

}
