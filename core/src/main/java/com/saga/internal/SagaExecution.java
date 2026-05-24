package com.saga.internal;

import com.saga.SagaPersistenceHook;
import com.saga.lifecycle.SagaLifecycleObserver;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
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

    SagaExecution(String name, List<SagaTransition<?, ?, ?>> transitions,
                  InputResolver<O> finalOutputResolver, Class<O> outputClass,
                  @Nullable Function<I, String> correlationIdExtractor,
                  @Nullable SagaPersistenceHook persistenceHook) {
        this.name = name;
        this.transitions = transitions;
        this.finalOutputResolver = finalOutputResolver;
        this.outputClass = outputClass;
        this.compensator = new SagaCompensator(name);
        this.correlationIdExtractor = correlationIdExtractor;
        this.persistenceHook = persistenceHook;
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

            Mono<SagaExecutionContext> contextMono = buildContext(initialPayload, correlationId);

            return contextMono.flatMap(context -> {
                int startIndex = context.getCompletedTransitions();
                // The chain starts from the last preloaded output (or the initial payload).
                List<Object> outputs = context.getExecutionOutputs();
                Object startValue = outputs.getLast();

                Mono<Object> chain = Mono.just(startValue != null ? startValue : CHAIN_SENTINEL);

                for (int i = 0; i < transitions.size(); i++) {
                    final int transitionIndex = i;
                    final SagaTransition<?, ?, ?> t = transitions.get(i);

                    if (transitionIndex < startIndex) {
                        // Transition already completed — skip execution.
                        // The context is pre-populated; the chain value passes through unchanged.
                    } else {
                        chain = chain.flatMap(input -> {
                            int sizeBefore = context.getExecutionOutputs().size();
                            Mono<Object> step = new SagaStepProcessor(t, obs, name, traceId)
                                    .processStep(input, context);
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

                return chain
                        .onErrorResume(err -> {
                            Mono<Object> compensated = compensator.compensate(context, err, obs);
                            if (persistenceHook == null) return compensated;
                            // compensated always terminates with an error, so use onErrorResume
                            // to call onFailed before re-propagating the compensation error.
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
        });
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
