package hamburg.engelmann.saga.engine.persistent;

/**
 * Thrown when a step output cannot be serialized or deserialized by a {@link SagaStateSerializer}.
 */
public class SagaSerializationException extends RuntimeException {

    public SagaSerializationException(String message, Throwable cause) {
        super(message, cause);
    }

}
