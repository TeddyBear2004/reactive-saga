package com.saga.internal;

import com.saga.Saga;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.LockableResourceId;
import com.saga.lock.SagaLockContext;
import com.saga.lock.SagaLockService;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;

/**
 * Concrete implementation of {@link Saga}.
 * Consumers interact only through the {@link Saga} interface.
 */
public class SagaImpl<I, O> implements Saga<I, O> {

    private final @Nullable SagaLifecycleObserver observer;
    private final SagaExecution<I, O> execution;
    private final @Nullable Function<I, List<LockableResourceId>> lockResourcesExtractor;
    private final @Nullable SagaLockService sagaLockService;

    public SagaImpl(String name,
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

    @Override
    public Mono<O> execute(I initialPayload) {
        return wrapWithLock(initialPayload, execution.execute(initialPayload, this.observer));
    }

    @Override
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

    @Override public String getName() { return execution.getName(); }
    @Override public Class<O> getOutputClass() { return execution.getOutputClass(); }

}
