package com.saga.internal;

import com.saga.lifecycle.SagaLifecycleObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

class SagaExecution<I, O> {

    private static final Logger log = LoggerFactory.getLogger(SagaExecution.class);

    private final String name;
    private final List<SagaTransition<?, ?, ?>> transitions;
    private final InputResolver<O> finalOutputResolver;
    private final Class<O> outputClass;
    private final SagaCompensator compensator;

    SagaExecution(String name, List<SagaTransition<?, ?, ?>> transitions,
                  InputResolver<O> finalOutputResolver, Class<O> outputClass) {
        this.name = name;
        this.transitions = transitions;
        this.finalOutputResolver = finalOutputResolver;
        this.outputClass = outputClass;
        this.compensator = new SagaCompensator(name);
    }

    String getName() { return name; }
    Class<O> getOutputClass() { return outputClass; }

    Mono<O> execute(I initialPayload, SagaLifecycleObserver obs) {
        return Mono.defer(() -> {
            UUID traceId = UUID.randomUUID();
            Instant start = Instant.now();
            log.info("[Saga:{}] Starting execution with trace ID: {}", name, traceId);
            if (obs != null) obs.onSagaStarted(name);

            SagaExecutionContext context = new SagaExecutionContext(initialPayload);
            Mono<Object> chain = Mono.just(initialPayload);

            for (SagaTransition<?, ?, ?> t : transitions) {
                chain = chain.flatMap(input ->
                        new SagaStepProcessor(t, obs, name, traceId).processStep(input, context));
            }

            return chain
                    .onErrorResume(err -> compensator.compensate(context, err, obs))
                    .doOnSuccess(_ -> {
                        log.info("[Saga:{}] Completed successfully in {}ms.", traceId,
                                 start.until(Instant.now()).toMillis());
                        if (obs != null) obs.onSagaCompleted(name);
                    })
                    .mapNotNull(_ -> finalOutputResolver.resolve(null, context.getExecutionOutputs()))
                    .cast(outputClass);
        });
    }

}
