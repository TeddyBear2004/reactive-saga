package com.saga.persistent;

/**
 * SPI for serializing and deserializing individual saga step outputs for persistence.
 *
 * <p>A default Jackson-based implementation can be provided by the caller:
 * <pre>{@code
 * SagaStateSerializer serializer = value -> objectMapper.writeValueAsBytes(value);
 * }</pre>
 *
 * <p>Implementations must be able to round-trip any step output type used in the saga.
 */
public interface SagaStateSerializer {

    /**
     * Serializes a step output value to a byte array.
     *
     * @throws SagaSerializationException if the value cannot be serialized
     */
    byte[] serialize(Object value);

    /**
     * Deserializes a byte array back to the given type.
     *
     * @throws SagaSerializationException if the bytes cannot be deserialized to the given type
     */
    <T> T deserialize(byte[] bytes, Class<T> type);

}
