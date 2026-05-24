package com.saga.internal;

import com.saga.lifecycle.CapturingSagaLifecycleObserver;
import com.saga.step.SagaCompensationException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Thread-safe implementation of {@link CapturingSagaLifecycleObserver}.
 * Obtain via {@link CapturingSagaLifecycleObserver#create()}.
 */
public class DefaultCapturingSagaLifecycleObserver implements CapturingSagaLifecycleObserver {

    private final List<String> startedSagas = new CopyOnWriteArrayList<>();
    private final List<StepEvent> startedSteps = new CopyOnWriteArrayList<>();
    private final List<StepEvent> completedSteps = new CopyOnWriteArrayList<>();
    private final List<StepErrorEvent> failedSteps = new CopyOnWriteArrayList<>();
    private final List<SagaErrorEvent> errors = new CopyOnWriteArrayList<>();
    private final List<String> completedSagas = new CopyOnWriteArrayList<>();
    private final List<String> compensatedSagas = new CopyOnWriteArrayList<>();
    private final List<SagaCompensationException> compensationFailures = new CopyOnWriteArrayList<>();

    @Override public void onSagaStarted(String s) { startedSagas.add(s); }
    @Override public void onStepStarted(String s, String n) { startedSteps.add(new StepEvent(s, n)); }
    @Override public void onStepCompleted(String s, String n) { completedSteps.add(new StepEvent(s, n)); }
    @Override public void onStepFailed(String s, String n, Throwable e) { failedSteps.add(new StepErrorEvent(s, n, e)); }
    @Override public void onError(String s, Throwable e) { errors.add(new SagaErrorEvent(s, e)); }
    @Override public void onSagaCompleted(String s) { completedSagas.add(s); }
    @Override public void onCompensationCompleted(String s) { compensatedSagas.add(s); }
    @Override public void onCompensationFailed(String s, SagaCompensationException e) { compensationFailures.add(e); }

    @Override public List<String> getStartedSagas() { return Collections.unmodifiableList(startedSagas); }
    @Override public List<StepEvent> getStartedSteps() { return Collections.unmodifiableList(startedSteps); }
    @Override public List<StepEvent> getCompletedSteps() { return Collections.unmodifiableList(completedSteps); }
    @Override public List<StepErrorEvent> getFailedSteps() { return Collections.unmodifiableList(failedSteps); }
    @Override public List<SagaErrorEvent> getErrors() { return Collections.unmodifiableList(errors); }
    @Override public List<String> getCompletedSagas() { return Collections.unmodifiableList(completedSagas); }
    @Override public List<String> getCompensatedSagas() { return Collections.unmodifiableList(compensatedSagas); }
    @Override public List<SagaCompensationException> getCompensationFailures() { return Collections.unmodifiableList(compensationFailures); }
    @Override public boolean hasErrors() { return !errors.isEmpty(); }
    @Override public boolean hasCompensationFailures() { return !compensationFailures.isEmpty(); }

    @Override
    public void reset() {
        startedSagas.clear(); startedSteps.clear(); completedSteps.clear(); failedSteps.clear();
        errors.clear(); completedSagas.clear(); compensatedSagas.clear(); compensationFailures.clear();
    }
}
