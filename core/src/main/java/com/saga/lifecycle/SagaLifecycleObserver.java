package com.saga.lifecycle;

import com.saga.internal.CombinedSagaLifecycleObserver;
import com.saga.step.SagaCompensationException;

import java.util.List;

/**
 * Observer notified about key saga lifecycle events.
 * All methods have default no-op implementations.
 */
public interface SagaLifecycleObserver {

    default void onSagaStarted(String sagaName) {}

    default void onStepStarted(String sagaName, String stepName) {}

    default void onStepCompleted(String sagaName, String stepName) {}

    default void onStepFailed(String sagaName, String stepName, Throwable error) {}

    default void onError(String sagaName, Throwable error) {}

    default void onSagaCompleted(String sagaName) {}

    default void onCompensationCompleted(String sagaName) {}

    default void onCompensationFailed(String sagaName, SagaCompensationException error) {}

    /** Combines multiple observers into one that delegates each event to all of them. */
    static SagaLifecycleObserver combine(SagaLifecycleObserver... observers) {
        return new CombinedSagaLifecycleObserver(List.of(observers));
    }

}
