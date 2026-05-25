package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.di.internal.InputResolver;
import hamburg.engelmann.saga.SagaPersistenceHook;
import hamburg.engelmann.saga.lifecycle.SagaLifecycleObserver;
import hamburg.engelmann.saga.lock.Lockable;
import hamburg.engelmann.saga.lock.SagaLock;
import hamburg.engelmann.saga.lock.SagaLockContext;
import hamburg.engelmann.saga.lock.SagaLockService;
import hamburg.engelmann.saga.lock.SagaStepContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

class SagaExecution<I, O> {

    private static final Logger log = LoggerFactory.getLogger(SagaExecution.class);
    /** Sentinel for null initial payloads (Void sagas) so Mono.just() never receives null. */
    private static final Object CHAIN_SENTINEL = new Object();

    private final String name;
    private final List<SagaTransition<?, ?, ?>> transitions;
    private final InputResolver<O> finalOutputResolver;
    private final Class<O> outputClass;
    private final SagaCompensator compensator;
    private final @Nullable Function<I, String> correlationIdExtractor;
    private final @Nullable SagaPersistenceHook persistenceHook;
    private final List<DeferredLockEntry> deferredLockEntries;
    private final @Nullable SagaLockService sagaLockService;

    SagaExecution(String name, List<SagaTransition<?, ?, ?>> transitions,
                  InputResolver<O> finalOutputResolver, Class<O> outputClass,
                  @Nullable Function<I, String> correlationIdExtractor,
                  @Nullable SagaPersistenceHook persistenceHook,
                  List<DeferredLockEntry> deferredLockEntries,
                  @Nullable SagaLockService sagaLockService) {
        this.name = name;
        this.transitions = transitions;
        this.finalOutputResolver = finalOutputResolver;
        this.outputClass = outputClass;
        this.compensator = new SagaCompensator(name);
        this.correlationIdExtractor = correlationIdExtractor;
        this.persistenceHook = persistenceHook;
        this.deferredLockEntries = deferredLockEntries;
        this.sagaLockService = sagaLockService;
    }

    String getName() { return name; }
    Class<O> getOutputClass() { return outputClass; }

    Mono<O> execute(I initialPayload, @Nullable SagaLifecycleObserver obs) {
        return Mono.defer(() -> {
            UUID traceId = UUID.randomUUID();
            String correlationId = (correlationIdExtractor != null)
                    ? correlationIdExtractor.apply(initialPayload)
                    : traceId.toString();
            Instant start = Instant.now();
            log.info("[Saga:{}] Starting execution correlationId={} traceId={}", name, correlationId, traceId);
            if (obs != null) obs.onSagaStarted(name);

            // Accumulates locks acquired by steps via SagaStepContext; thread-safe for parallel steps.
            List<SagaLock> stepLocks = new CopyOnWriteArrayList<>();
            @Nullable SagaLockContext stepLockCtx = (sagaLockService != null)
                    ? SagaLockContext.forSaga(name) : null;

            Mono<SagaExecutionContext> contextMono = buildContext(initialPayload, correlationId);

            Mono<O> chain = contextMono.flatMap(context -> {
                int startIndex = context.getCompletedTransitions();
                List<Object> outputs = context.getExecutionOutputs();
                Object startValue = outputs.getLast();

                Mono<Object> mainChain = buildMainChain(
                        0,
                        Mono.just(startValue != null ? startValue : CHAIN_SENTINEL),
                        context, obs, traceId, startIndex, correlationId, initialPayload,
                        stepLockCtx, stepLocks);

                return mainChain
                        .onErrorResume(err -> {
                            Mono<Object> compensated = compensator.compensate(context, err, obs);
                            if (persistenceHook == null) return compensated;
                            return compensated.onErrorResume(compensationErr ->
                                    persistenceHook.onFailed(name, correlationId)
                                            .then(Mono.error(compensationErr)));
                        })
                        .doOnSuccess(_ -> log.info("[Saga:{}] Completed successfully in {}ms. correlationId={}",
                                                   name, start.until(Instant.now()).toMillis(), correlationId))
                        .mapNotNull(chainValue -> finalOutputResolver.resolve(chainValue, context.getExecutionOutputs()))
                        .cast(outputClass)
                        .flatMap(finalOutput -> {
                            if (obs != null) obs.onSagaCompleted(name);
                            if (persistenceHook == null) return Mono.just(finalOutput);
                            return persistenceHook.onCompleted(name, correlationId).thenReturn(finalOutput);
                        });
            });

            // Release or fail step-acquired locks after saga completion.
            // Only calls the lock service if steps actually acquired locks via SagaStepContext.
            if (sagaLockService == null) return chain;
            return chain
                    .flatMap(r -> stepLocks.isEmpty()
                            ? Mono.just(r)
                            : sagaLockService.releaseAcquiredLocks(stepLocks).thenReturn(r))
                    .onErrorResume(err -> stepLocks.isEmpty()
                            ? Mono.error(err)
                            : sagaLockService.failAcquiredLocks(stepLocks).then(Mono.error(err)));
        });
    }

    private Mono<Object> buildMainChain(int fromIndex, Mono<Object> start,
                                        SagaExecutionContext context,
                                        @Nullable SagaLifecycleObserver obs,
                                        UUID traceId, int startIndex, String correlationId,
                                        I initialPayload,
                                        @Nullable SagaLockContext stepLockCtx,
                                        @Nullable List<SagaLock> stepLockAccumulator) {
        if (fromIndex >= transitions.size()) return start;

        OptionalInt minLockAt = deferredLockEntries.stream()
                .mapToInt(DeferredLockEntry::afterTransitionIndex)
                .filter(idx -> idx >= fromIndex)
                .min();

        if (minLockAt.isEmpty()) {
            return buildSegment(fromIndex, transitions.size(), start, context, obs, traceId,
                                startIndex, correlationId, stepLockCtx, stepLockAccumulator);
        }

        int lockAt = minLockAt.getAsInt();

        Mono<Object> upToLock = buildSegment(fromIndex, lockAt + 1, start, context, obs, traceId,
                                              startIndex, correlationId, stepLockCtx, stepLockAccumulator);

        List<DeferredLockEntry> locksAtIndex = deferredLockEntries.stream()
                .filter(e -> e.afterTransitionIndex() == lockAt)
                .toList();

        return upToLock.flatMap(output -> {
            var resources = locksAtIndex.stream()
                    .flatMap(entry -> {
                        var resolver = entry.resolver();
                        var spec = resolver.resolve(initialPayload, context.getExecutionOutputs());
                        return spec.lockableResources().stream().map(Lockable::getId);
                    })
                    .toList();

            Mono<Object> remaining = buildMainChain(lockAt + 1, Mono.just(output),
                                                    context, obs, traceId, startIndex, correlationId,
                                                    initialPayload, stepLockCtx, stepLockAccumulator);
            if (sagaLockService == null) return remaining;
            return sagaLockService.withLock(SagaLockContext.forSaga(name), resources, remaining);
        });
    }

    /** Builds a flat sequential chain for transitions in the range {@code [from, to)}. */
    private Mono<Object> buildSegment(int from, int to, Mono<Object> start,
                                      SagaExecutionContext context,
                                      @Nullable SagaLifecycleObserver obs,
                                      UUID traceId, int startIndex, String correlationId,
                                      @Nullable SagaLockContext stepLockCtx,
                                      @Nullable List<SagaLock> stepLockAccumulator) {
        Mono<Object> chain = start;
        for (int i = from; i < to; i++) {
            final int transitionIndex = i;
            final SagaTransition<?, ?, ?> t = transitions.get(i);

            if (transitionIndex < startIndex) {
                // Transition already completed — skip execution.
            } else {
                chain = chain.flatMap(input -> {
                    int sizeBefore = context.getExecutionOutputs().size();
                    SagaStepContext stepContext = (sagaLockService != null
                            && stepLockCtx != null
                            && stepLockAccumulator != null)
                            ? new DefaultSagaStepContext(sagaLockService, stepLockCtx, stepLockAccumulator)
                            : null;
                    Mono<Object> step = new SagaStepProcessor(t, obs, name, traceId)
                            .processStep(input, context, stepContext);
                    if (persistenceHook == null) return step;
                    return step.flatMap(result -> {
                        int sizeAfter = context.getExecutionOutputs().size();
                        List<Object> diff = context.getExecutionOutputs()
                                .subList(sizeBefore, sizeAfter);
                        return persistenceHook
                                .afterStep(name, correlationId, transitionIndex,
                                           Collections.unmodifiableList(diff))
                                .thenReturn(result);
                    });
                });
            }
        }
        return chain;
    }

    private Mono<SagaExecutionContext> buildContext(I initialPayload, String correlationId) {
        if (persistenceHook == null) {
            return Mono.just(new SagaExecutionContext(initialPayload));
        }
        return persistenceHook.loadResumePoint(name, correlationId)
                .map(rp -> new SagaExecutionContext(initialPayload,
                                                    rp.preloadedOutputs(),
                                                    rp.completedTransitions()))
                .defaultIfEmpty(new SagaExecutionContext(initialPayload));
    }

}

