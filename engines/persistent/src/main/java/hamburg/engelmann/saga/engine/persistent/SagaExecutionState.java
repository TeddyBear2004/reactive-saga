package hamburg.engelmann.saga.engine.persistent;

import java.time.Instant;
import java.util.List;

/**
 * Serialized snapshot of a saga execution stored after every completed step.
 *
 * <p>Step outputs are stored as raw bytes — serialized by a {@link SagaStateSerializer}.
 * On crash recovery the engine deserializes them using the step's declared output type
 * to reconstruct the execution context from {@code currentStepIndex} onwards.
 *
 * @param id                     uniquely identifies this execution instance
 * @param status                 current lifecycle status
 * @param currentStepIndex       0-based index of the next step to execute (0 = not started)
 * @param serializedStepOutputs  outputs of already-completed steps, in order
 * @param createdAt              when this execution was first persisted
 * @param updatedAt              when this execution was last modified
 */
public record SagaExecutionState(
        SagaExecutionId id,
        SagaExecutionStatus status,
        int currentStepIndex,
        List<byte[]> serializedStepOutputs,
        Instant createdAt,
        Instant updatedAt
) {}
