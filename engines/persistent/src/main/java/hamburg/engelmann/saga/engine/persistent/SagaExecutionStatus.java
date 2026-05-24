package hamburg.engelmann.saga.engine.persistent;

/** Lifecycle status of a persistent saga execution. */
public enum SagaExecutionStatus {

    /** Execution has started but has not yet completed all steps. */
    IN_PROGRESS,

    /** All steps completed successfully. */
    COMPLETED,

    /** A step failed and compensation has not yet run. */
    FAILED,

    /** Compensation has been executed (regardless of whether it succeeded). */
    COMPENSATED

}
