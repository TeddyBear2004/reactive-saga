package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.lifecycle.SagaLifecycleObserver;
import hamburg.engelmann.saga.step.StepResult;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

class SagaStepProcessor {

    private static final Logger log = LoggerFactory.getLogger(SagaStepProcessor.class);

    private final SagaTransition<?, ?, ?> transition;
    private final @Nullable SagaLifecycleObserver obs;
    private final String sagaName;
    private final UUID sagaTraceId;

    SagaStepProcessor(SagaTransition<?, ?, ?> transition, @Nullable SagaLifecycleObserver obs,
                      String sagaName, UUID sagaTraceId) {
        this.transition = transition;
        this.obs = obs;
        this.sagaName = sagaName;
        this.sagaTraceId = sagaTraceId;
    }

    Mono<Object> processStep(Object currentInput, SagaExecutionContext context) {
        log.debug("[Saga:{}] Executing step: {}", sagaTraceId, transition.stepName());
        if (obs != null) obs.onStepStarted(sagaName, transition.stepName());

        Mono<StepResult<Object, Object>> stepMono = Mono.defer(
                () -> transition.executeValidated(currentInput, context.getExecutionOutputs()));

        Duration stepTimeout = transition.timeout();
        if (stepTimeout != null) stepMono = stepMono.timeout(stepTimeout);

        if (transition.retrySpec() != null) stepMono = stepMono.retryWhen(transition.retrySpec());

        return stepMono
                .doOnNext(result -> handleSuccess(result, context))
                .doOnError(this::handleError)
                .flatMap(this::extractOutput);
    }

    private void handleSuccess(StepResult<?, ?> result, SagaExecutionContext context) {
        if (result == null) {
            log.warn("[Saga:{}] Step '{}' returned null result.", sagaTraceId, transition.stepName());
            context.addNullOutput();
            return;
        }
        context.addRollbackRecord(transition, result.localState());
        if (result.output() != null) {
            context.addOutput(result.output());
        } else {
            log.warn("[Saga:{}] Step '{}' returned null output.", sagaTraceId, transition.stepName());
        }
        if (obs != null) obs.onStepCompleted(sagaName, transition.stepName());
        log.debug("[Saga:{}] Step completed: {}", sagaTraceId, transition.stepName());
    }

    private void handleError(Throwable throwable) {
        log.error("[Saga:{}] Step '{}' failed: {}", sagaTraceId, transition.stepName(), throwable.getMessage());
        if (obs != null) obs.onStepFailed(sagaName, transition.stepName(), throwable);
    }

    private Mono<Object> extractOutput(StepResult<?, ?> result) {
        return Mono.just(Objects.requireNonNullElse(result.output(), StepResult.EMPTY_OUTPUT));
    }

}
