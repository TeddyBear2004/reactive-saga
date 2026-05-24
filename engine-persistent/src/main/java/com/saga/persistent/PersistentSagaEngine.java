package com.saga.persistent;

import com.saga.Saga;
import com.saga.SagaBuilder;
import com.saga.SagaEngine;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.SagaLockService;
import org.jspecify.annotations.Nullable;

/**
 * {@link SagaEngine} that persists execution state after every completed step,
 * enabling crash recovery and durable distributed saga execution.
 *
 * <p>Usage:
 * <pre>{@code
 * SagaEngine engine = new PersistentSagaEngine(repository, serializer);
 *
 * Saga<Order, Receipt> saga = engine.build(
 *         Saga.builder("OrderFlow", Order.class, Receipt.class)
 *                 .step(new ValidateOrderStep())
 *                 .step(new ReserveInventoryStep())
 * );
 * }</pre>
 *
 * <p><strong>Note:</strong> Step-by-step state persistence and crash-recovery resume logic are
 * not yet implemented. This class is a scaffold defining the public API and SPIs.
 * Use {@link SagaEngine#inMemory()} for current production usage.
 *
 * @see SagaExecutionRepository
 * @see SagaStateSerializer
 */
public final class PersistentSagaEngine implements SagaEngine {

    private final SagaExecutionRepository repository;
    private final SagaStateSerializer serializer;
    private final @Nullable SagaLifecycleObserver observer;
    private final @Nullable SagaLockService lockService;

    public PersistentSagaEngine(SagaExecutionRepository repository, SagaStateSerializer serializer) {
        this(repository, serializer, null, null);
    }

    private PersistentSagaEngine(SagaExecutionRepository repository,
                                  SagaStateSerializer serializer,
                                  @Nullable SagaLifecycleObserver observer,
                                  @Nullable SagaLockService lockService) {
        this.repository = repository;
        this.serializer = serializer;
        this.observer = observer;
        this.lockService = lockService;
    }

    @Override
    public SagaEngine withObserver(SagaLifecycleObserver observer) {
        return new PersistentSagaEngine(repository, serializer, observer, lockService);
    }

    @Override
    public SagaEngine withLockService(SagaLockService lockService) {
        return new PersistentSagaEngine(repository, serializer, observer, lockService);
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException always — persistent execution is not yet implemented
     */
    @Override
    public <I, O> Saga<I, O> build(SagaBuilder<I, O> builder) {
        // TODO: implement PersistentSagaImpl
        //   1. Check SagaExecutionRepository for an existing IN_PROGRESS execution (resume path)
        //   2. After each step, serialize the output and save SagaExecutionState via repository
        //   3. On error, persist FAILED status and trigger compensation
        //   4. On success, persist COMPLETED status (or delete if ephemeral cleanup is preferred)
        //   Serialization design decision needed: step output types must be known at resume time.
        throw new UnsupportedOperationException(
                "PersistentSagaEngine is not yet implemented. Use SagaEngine.inMemory() instead."
        );
    }

}
