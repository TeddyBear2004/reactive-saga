package com.saga.lifecycle;

import com.saga.internal.DefaultCapturingSagaLifecycleObserver;
import com.saga.step.SagaCompensationException;

import java.util.List;

/**
 * A {@link SagaLifecycleObserver} that captures all lifecycle events in memory.
 * Designed for use in tests to assert that specific saga events occurred.
 *
 * <p>Example:
 * <pre>{@code
 * CapturingSagaLifecycleObserver observer = CapturingSagaLifecycleObserver.create();
 * saga.execute(input, observer).block();
 * assertThat(observer.getCompletedSagas()).containsExactly("MyFlowName");
 * }</pre>
 */
public interface CapturingSagaLifecycleObserver extends SagaLifecycleObserver {

    record StepEvent(String sagaName, String stepName) {}

    record SagaErrorEvent(String sagaName, Throwable error) {}

    record StepErrorEvent(String sagaName, String stepName, Throwable error) {}

    List<String> getStartedSagas();

    List<StepEvent> getStartedSteps();

    List<StepEvent> getCompletedSteps();

    List<StepErrorEvent> getFailedSteps();

    List<SagaErrorEvent> getErrors();

    List<String> getCompletedSagas();

    List<String> getCompensatedSagas();

    List<SagaCompensationException> getCompensationFailures();

    boolean hasErrors();

    boolean hasCompensationFailures();

    void reset();

    static CapturingSagaLifecycleObserver create() {
        return new DefaultCapturingSagaLifecycleObserver();
    }

}
