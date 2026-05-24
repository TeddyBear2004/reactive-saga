package hamburg.engelmann.saga.engine.persistent;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * SPI for serializing and deserializing individual saga step outputs for persistence.
 *
 * <p>A default Jackson FQCN-based implementation is available:
 * <pre>{@code
 * SagaStateSerializer serializer = SagaStateSerializer.jacksonWithFqcn(objectMapper);
 * }</pre>
 *
 * <p>Implementations must be able to round-trip any step output type used in the saga.
 */
public interface SagaStateSerializer {

    /**
     * Serializes a step output value to a byte array.
     * May return {@code null} for a {@code null} input value.
     *
     * @throws SagaSerializationException if the value cannot be serialized
     */
    byte[] serialize(Object value);

    /**
     * Deserializes a byte array back to the given type.
     * May return {@code null} when {@code bytes} is {@code null}.
     *
     * @throws SagaSerializationException if the bytes cannot be deserialized to the given type
     */
    <T> T deserialize(byte[] bytes, Class<T> type);

    /**
     * Creates a serializer that embeds the FQCN as a {@code @class} JSON property in every value,
     * enabling automatic type recovery without knowing the target type at deserialization time.
     *
     * <p>The provided {@link ObjectMapper} is <em>copied</em> and extended with type info;
     * the original mapper is not mutated.
     *
     * @see JacksonSagaStateSerializer
     */
    static SagaStateSerializer jacksonWithFqcn(ObjectMapper mapper) {
        return new JacksonSagaStateSerializer(mapper);
    }

}
