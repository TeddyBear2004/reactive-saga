package com.saga.internal;

import com.saga.Saga;
import com.saga.SagaPersistenceHook;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.LockableResourceId;
import com.saga.lock.SagaLockContext;
import com.saga.lock.SagaLockService;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.time.Duration;
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
    private final @Nullable Duration sagaTimeout;

    public SagaImpl(String name,
                    List<SagaTransition<?, ?, ?>> transitions,
                    Class<O> outputClass,
                    @Nullable SagaLifecycleObserver observer,
                    InputResolver<O> finalOutputResolver,
                    @Nullable Function<I, List<LockableResourceId>> lockResourcesExtractor,
                    @Nullable SagaLockService sagaLockService,
                    @Nullable Function<I, String> correlationIdExtractor,
                    @Nullable SagaPersistenceHook persistenceHook,
                    @Nullable Duration sagaTimeout) {
        this.observer = observer;
        this.execution = new SagaExecution<>(name, transitions, finalOutputResolver, outputClass,
                                             correlationIdExtractor, persistenceHook);
        this.lockResourcesExtractor = lockResourcesExtractor;
        this.sagaLockService = sagaLockService;
        this.sagaTimeout = sagaTimeout;
    }

    @Override
    public Mono<O> execute(I initialPayload) {
        Mono<O> mono = execution.execute(initialPayload, this.observer);
        if (sagaTimeout != null) mono = mono.timeout(sagaTimeout);
        return wrapWithLock(initialPayload, mono);
    }

    @Override
    public Mono<O> execute(I initialPayload, SagaLifecycleObserver additionalObserver) {
        SagaLifecycleObserver combined = (this.observer != null)
                ? SagaLifecycleObserver.combine(this.observer, additionalObserver)
                : additionalObserver;
        Mono<O> mono = execution.execute(initialPayload, combined);
        if (sagaTimeout != null) mono = mono.timeout(sagaTimeout);
        return wrapWithLock(initialPayload, mono);
    }

    private Mono<O> wrapWithLock(I input, Mono<O> mono) {
        if (sagaLockService == null || lockResourcesExtractor == null) return mono;
        return sagaLockService.withLock(SagaLockContext.forSaga(execution.getName()),
                                        lockResourcesExtractor.apply(input), mono);
    }

    @Override public String getName() { return execution.getName(); }
    @Override public Class<O> getOutputClass() { return execution.getOutputClass(); }

}
