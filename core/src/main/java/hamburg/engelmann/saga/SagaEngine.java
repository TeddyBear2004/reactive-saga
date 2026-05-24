package hamburg.engelmann.saga;

import hamburg.engelmann.saga.lifecycle.SagaLifecycleObserver;
import hamburg.engelmann.saga.lock.SagaLockService;

/**
 * Central execution-strategy abstraction. An engine is a shared, immutable configuration context
 * that creates {@link Saga} instances with a specific runtime behaviour (e.g. in-memory, persistent).
 *
 * <p>Typical usage:
 * <pre>{@code
 * SagaEngine engine = InMemorySagaEngine.create()
 *         .withObserver(metricsObserver)
 *         .withLockService(lockService);
 *
 * Saga<Order, Receipt> saga = engine.build(
 *         Saga.builder("OrderFlow", Order.class, Receipt.class)
 *                 .step(new ValidateOrderStep())
 *                 .step(new ReserveInventoryStep())
 * );
 * }</pre>
 *
 * <p>Engine-level configuration (observer, lock service) is applied to every {@link Saga} built from
 * the engine, and overrides any equivalent settings on the builder.
 *
 * @see hamburg.engelmann.engine.memory.InMemorySagaEngine
 * @see hamburg.engelmann.engine.persistent.PersistentSagaEngine
 */
public interface SagaEngine {

    /**
     * Returns a new engine with the given observer applied to every saga it builds.
     * Replaces any previously configured observer.
     */
    SagaEngine withObserver(SagaLifecycleObserver observer);

    /**
     * Returns a new engine that injects the given lock service into every saga it builds.
     * Replaces any previously configured lock service.
     */
    SagaEngine withLockService(SagaLockService lockService);

    /**
     * Builds a {@link Saga} from the given builder, applying the engine's execution strategy
     * and shared configuration (observer, lock service).
     */
    <I, O> Saga<I, O> build(SagaBuilder<I, O> builder);

}
