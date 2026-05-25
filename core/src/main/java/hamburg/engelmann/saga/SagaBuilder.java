package hamburg.engelmann.saga;

import hamburg.engelmann.saga.lifecycle.SagaLifecycleObserver;
import hamburg.engelmann.saga.lock.LockSpec;
import hamburg.engelmann.saga.lock.SagaLockService;
import hamburg.engelmann.saga.step.SagaStep;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
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

    /**
     * Adds exclusive resource locking resolved via the same DI mechanism as step inputs.
     *
     * <p>The {@code lockSpecClass} must be an interface or record whose fields/methods can be
     * mapped from the types available at the point of this call (typically the initial input).
     * Identical to how step input types are resolved — no extractor function needed.
     * Can be called multiple times; all resources accumulate.
     *
     * <pre>{@code
     * record OrderLocks(OrderId orderId, UserId userId) implements LockSpec {
     *     public List<Lockable> lockableResources() { return List.of(orderId, userId); }
     * }
     *
     * Saga.builder("ProcessOrder", CreateOrderInput.class, Receipt.class)
     *     .withLock(OrderLocks.class)
     *     ...
     * }</pre>
     */
    SagaBuilder<I, O> withLock(Class<? extends LockSpec> lockSpecClass);

    /**
     * Adds exclusive resource locking around every {@link Saga#execute} call.
     *
     * <p>Can be called multiple times — each call contributes additional resources to lock.
     * Prefer {@link #withLock(Class)} when the spec can be derived from the available context.
     *
     * @param lockSpecExtractor extracts a {@link LockSpec} from the initial input;
     *                          the spec lists the individual {@link hamburg.engelmann.saga.lock.Lockable} resources to lock
     */
    SagaBuilder<I, O> withLock(Function<I, ? extends LockSpec> lockSpecExtractor);

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

    /**
     * Sets a timeout for the entire saga execution.
     * If the saga does not complete within this duration, a {@link java.util.concurrent.TimeoutException}
     * is propagated and compensation is triggered.
     *
     * @param sagaTimeout maximum duration for the whole saga
     */
    SagaBuilder<I, O> timeout(Duration sagaTimeout);

    /**
     * Adds a step to the saga and returns a {@link SagaStepBuilder} that allows setting a
     * per-step timeout via {@link SagaStepBuilder#timeout}.
     */
    <NI, NO, L> SagaStepBuilder<I, O> step(SagaStep<NI, NO, L> step);

    /**
     * Adds a parallel step group and returns a {@link SagaStepBuilder} that allows setting a
     * per-group timeout via {@link SagaStepBuilder#timeout}.
     */
    SagaStepBuilder<I, O> parallel(String groupName, SagaStep<?, ?, ?>... subSteps);

    /**
     * Adds a parallel step group and returns a {@link SagaStepBuilder} that allows setting a
     * per-group timeout via {@link SagaStepBuilder#timeout}.
     */
    SagaStepBuilder<I, O> parallel(String groupName, List<SagaStep<?, ?, ?>> subSteps);

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
