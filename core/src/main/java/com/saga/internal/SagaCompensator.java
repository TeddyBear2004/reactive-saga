package com.saga.internal;

import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.step.SagaCompensationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

class SagaCompensator {

    private static final Logger log = LoggerFactory.getLogger(SagaCompensator.class);

    private final String sagaName;

    SagaCompensator(String sagaName) { this.sagaName = sagaName; }

    Mono<Object> compensate(SagaExecutionContext context, Throwable originalError, SagaLifecycleObserver obs) {
        log.error("[Saga:{}] Failed. Starting compensation of {} steps. Cause: {}",
                  sagaName, context.getRollbackStackSize(), originalError.getMessage());
        if (obs != null) {
            try { obs.onError(sagaName, originalError); }
            catch (Exception e) { log.error("[Saga:{}] Observer threw during onError: {}", sagaName, e.getMessage(), e); }
        }

        return Flux.fromIterable(context.getReversedRollbackStack())
                .concatMap(record -> record.transition().compensateValidated(record.localState())
                        .thenReturn(Optional.<SagaCompensationException.CompensationStepFailure>empty())
                        .onErrorResume(err -> {
                            log.error("[Saga:{}] FATAL: Compensation step '{}' failed!", sagaName, record.transition().stepName(), err);
                            return Mono.just(Optional.of(new SagaCompensationException.CompensationStepFailure(
                                    record.transition().stepName(), err)));
                        }))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collectList()
                .flatMap(failures -> {
                    if (failures.isEmpty()) {
                        if (obs != null) obs.onCompensationCompleted(sagaName);
                        return Mono.error(originalError);
                    }
                    SagaCompensationException ex = new SagaCompensationException(sagaName, originalError, failures);
                    if (obs != null) obs.onCompensationFailed(sagaName, ex);
                    return Mono.error(ex);
                });
    }

}
