package com.saga;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Thread-safe {@link SagaLifecycleObserver} that records all lifecycle events in memory.
 * Designed for use in tests to assert that specific saga events occurred.
 *
 * <p>Example:
 * <pre>{@code
 * CapturingSagaLifecycleObserver observer = new CapturingSagaLifecycleObserver();
 * saga.execute(input, observer).block();
 * assertThat(observer.getCompletedSagas()).containsExactly("MyFlowName");
 * }</pre>
 */
public class CapturingSagaLifecycleObserver implements SagaLifecycleObserver {

    private final List<String> startedSagas = new CopyOnWriteArrayList<>();
    private final List<StepEvent> completedSteps = new CopyOnWriteArrayList<>();
    private final List<StepErrorEvent> failedSteps = new CopyOnWriteArrayList<>();
    private final List<SagaErrorEvent> errors = new CopyOnWriteArrayList<>();
    private final List<String> completedSagas = new CopyOnWriteArrayList<>();
    private final List<String> compensatedSagas = new CopyOnWriteArrayList<>();
    private final List<SagaCompensationException> compensationFailures = new CopyOnWriteArrayList<>();

    public record StepEvent(String sagaName, String stepName) {}
    public record SagaErrorEvent(String sagaName, Throwable error) {}
    public record StepErrorEvent(String sagaName, String stepName, Throwable error) {}

    @Override public void onSagaStarted(String s) { startedSagas.add(s); }
    @Override public void onStepCompleted(String s, String n) { completedSteps.add(new StepEvent(s, n)); }
    @Override public void onStepFailed(String s, String n, Throwable e) { failedSteps.add(new StepErrorEvent(s, n, e)); }
    @Override public void onError(String s, Throwable e) { errors.add(new SagaErrorEvent(s, e)); }
    @Override public void onSagaCompleted(String s) { completedSagas.add(s); }
    @Override public void onCompensationCompleted(String s) { compensatedSagas.add(s); }
    @Override public void onCompensationFailed(String s, SagaCompensationException e) { compensationFailures.add(e); }

    public List<String> getStartedSagas() { return Collections.unmodifiableList(startedSagas); }
    public List<StepEvent> getCompletedSteps() { return Collections.unmodifiableList(completedSteps); }
    public List<StepErrorEvent> getFailedSteps() { return Collections.unmodifiableList(failedSteps); }
    public List<SagaErrorEvent> getErrors() { return Collections.unmodifiableList(errors); }
    public List<String> getCompletedSagas() { return Collections.unmodifiableList(completedSagas); }
    public List<String> getCompensatedSagas() { return Collections.unmodifiableList(compensatedSagas); }
    public List<SagaCompensationException> getCompensationFailures() { return Collections.unmodifiableList(compensationFailures); }
    public boolean hasErrors() { return !errors.isEmpty(); }
    public boolean hasCompensationFailures() { return !compensationFailures.isEmpty(); }

    public void reset() {
        startedSagas.clear(); completedSteps.clear(); failedSteps.clear();
        errors.clear(); completedSagas.clear(); compensatedSagas.clear(); compensationFailures.clear();
    }
}
