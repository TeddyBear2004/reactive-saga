package hamburg.engelmann.saga.spring;

import hamburg.engelmann.saga.lifecycle.SagaLifecycleObserver;
import hamburg.engelmann.saga.step.SagaCompensationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SagaLifecycleObserver} that logs all saga lifecycle events via SLF4J at DEBUG level.
 *
 * <p>Auto-configured as a default bean by {@link SagaAutoConfiguration} when no other
 * {@link SagaLifecycleObserver} bean is present and the property
 * {@code saga.observer.logging.enabled} is {@code true} (the default).
 *
 * <p>To disable this observer, set:
 * <pre>{@code
 * saga.observer.logging.enabled=false
 * }</pre>
 *
 * <p>To replace it with a custom observer, simply register a {@link SagaLifecycleObserver} bean:
 * <pre>{@code
 * @Bean
 * public SagaLifecycleObserver myObserver() {
 *     return new MyCustomSagaLifecycleObserver();
 * }
 * }</pre>
 */
public class LoggingSagaLifecycleObserver implements SagaLifecycleObserver {

    private static final Logger log = LoggerFactory.getLogger(LoggingSagaLifecycleObserver.class);

    @Override
    public void onSagaStarted(String sagaName) {
        log.debug("[saga:{}] started", sagaName);
    }

    @Override
    public void onStepStarted(String sagaName, String stepName) {
        log.debug("[saga:{}] step '{}' started", sagaName, stepName);
    }

    @Override
    public void onStepCompleted(String sagaName, String stepName) {
        log.debug("[saga:{}] step '{}' completed", sagaName, stepName);
    }

    @Override
    public void onStepFailed(String sagaName, String stepName, Throwable error) {
        log.debug("[saga:{}] step '{}' failed: {}", sagaName, stepName, error.getMessage());
    }

    @Override
    public void onError(String sagaName, Throwable error) {
        log.debug("[saga:{}] error: {}", sagaName, error.getMessage());
    }

    @Override
    public void onSagaCompleted(String sagaName) {
        log.debug("[saga:{}] completed", sagaName);
    }

    @Override
    public void onCompensationCompleted(String sagaName) {
        log.debug("[saga:{}] compensation completed", sagaName);
    }

    @Override
    public void onCompensationFailed(String sagaName, SagaCompensationException error) {
        log.debug("[saga:{}] compensation failed: {}", sagaName, error.getMessage());
    }

}
