package hamburg.engelmann.saga.step;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Thrown when one or more compensation steps fail during saga rollback.
 */
public class SagaCompensationException extends RuntimeException {

    private final String sagaName;
    private final transient Throwable originalError;
    private final transient List<CompensationStepFailure> compensationFailures;

    public SagaCompensationException(String sagaName, Throwable originalError, List<CompensationStepFailure> compensationFailures) {
        super(buildMessage(sagaName, compensationFailures), originalError);
        this.sagaName = sagaName;
        this.originalError = originalError;
        this.compensationFailures = List.copyOf(compensationFailures);
    }

    public String getSagaName() { return sagaName; }
    public Throwable getOriginalError() { return originalError; }
    public List<CompensationStepFailure> getCompensationFailures() { return compensationFailures; }

    private static String buildMessage(String sagaName, List<CompensationStepFailure> failures) {
        String steps = failures.stream()
                .map(f -> f.stepName() + " → " + f.cause().getMessage())
                .collect(Collectors.joining(", "));
        return "[Saga:" + sagaName + "] " + failures.size()
               + " compensation step(s) failed — system state may be inconsistent: " + steps;
    }

    public record CompensationStepFailure(String stepName, Throwable cause) {}

}
