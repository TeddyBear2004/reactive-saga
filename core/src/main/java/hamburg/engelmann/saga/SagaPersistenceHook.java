package hamburg.engelmann.saga;

import reactor.core.publisher.Mono;

import java.util.List;

/**
 * SPI for persisting and resuming saga execution state.
 *
 * <p>Called by the execution engine to checkpoint step outputs and, on the next execution
 * with the same {@code (sagaName, correlationId)}, to restore those outputs so completed
 * steps are skipped and execution resumes from where it left off.
 *
 * <p>Obtain instances from {@link com.saga.engine.persistent.PersistentSagaEngine}.
 */
public interface SagaPersistenceHook {

    /**
     * Called immediately after a transition (step or parallel group) completes.
     *
     * <p>{@code transitionIndex} is 0-based (first step = 0).
     * {@code stepOutputs} contains every output added to the execution context by this transition:
     * one item for sequential steps, N items for a parallel group, or {@code null} for empty output.
     */
    Mono<Void> afterStep(String sagaName, String correlationId,
                         int transitionIndex, List<Object> stepOutputs);

    /**
     * Called before execution starts to check for a resumable in-progress run.
     *
     * <p>Returns a {@link SagaResumePoint} when a previous execution was interrupted,
     * or {@link Mono#empty()} to start a fresh run.
     */
    Mono<SagaResumePoint> loadResumePoint(String sagaName, String correlationId);

    /**
     * Called after all steps have completed and the final output has been resolved successfully.
     * Default implementation is a no-op.
     */
    default Mono<Void> onCompleted(String sagaName, String correlationId) {
        return Mono.empty();
    }

    /**
     * Called when the saga fails (after compensation has run or been attempted).
     * Default implementation is a no-op.
     */
    default Mono<Void> onFailed(String sagaName, String correlationId) {
        return Mono.empty();
    }

}
