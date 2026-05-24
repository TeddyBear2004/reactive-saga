package com.saga;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class CombinedSagaLifecycleObserver implements SagaLifecycleObserver {

    private static final Logger log = LoggerFactory.getLogger(CombinedSagaLifecycleObserver.class);

    private final List<SagaLifecycleObserver> observers;

    CombinedSagaLifecycleObserver(List<SagaLifecycleObserver> observers) {
        this.observers = observers;
    }

    @Override public void onSagaStarted(String n) { observers.forEach(o -> call(() -> o.onSagaStarted(n), "onSagaStarted")); }
    @Override public void onStepStarted(String s, String n) { observers.forEach(o -> call(() -> o.onStepStarted(s, n), "onStepStarted")); }
    @Override public void onStepCompleted(String s, String n) { observers.forEach(o -> call(() -> o.onStepCompleted(s, n), "onStepCompleted")); }
    @Override public void onStepFailed(String s, String n, Throwable e) { observers.forEach(o -> call(() -> o.onStepFailed(s, n, e), "onStepFailed")); }
    @Override public void onError(String s, Throwable e) { observers.forEach(o -> call(() -> o.onError(s, e), "onError")); }
    @Override public void onSagaCompleted(String n) { observers.forEach(o -> call(() -> o.onSagaCompleted(n), "onSagaCompleted")); }
    @Override public void onCompensationCompleted(String n) { observers.forEach(o -> call(() -> o.onCompensationCompleted(n), "onCompensationCompleted")); }
    @Override public void onCompensationFailed(String n, SagaCompensationException e) { observers.forEach(o -> call(() -> o.onCompensationFailed(n, e), "onCompensationFailed")); }

    private void call(Runnable fn, String name) {
        try { fn.run(); } catch (Exception e) { log.warn("Observer method {} threw an exception", name, e); }
    }
}
