package com.saga.persistent;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;

/**
 * Jackson-based serializer that embeds the FQCN as a {@code @class} property in every value.
 *
 * <p>This allows step outputs to be deserialized as {@code Object.class} without knowing the
 * concrete type in advance — Jackson resolves the type automatically from the embedded class name.
 *
 * <p><b>Security note:</b> {@code allowIfBaseType(Object.class)} is intentionally permissive so
 * that any application-defined class can round-trip through persistence. In security-sensitive
 * deployments consider providing a custom {@link ObjectMapper} pre-configured with a restricted
 * {@link BasicPolymorphicTypeValidator} that whitelists only known packages.
 *
 * <p>Obtain instances via {@link SagaStateSerializer#jacksonWithFqcn(ObjectMapper)}.
 */
class JacksonSagaStateSerializer implements SagaStateSerializer {

    private final ObjectMapper mapper;

    JacksonSagaStateSerializer(ObjectMapper source) {
        this.mapper = source.copy()
                .activateDefaultTypingAsProperty(
                        BasicPolymorphicTypeValidator.builder()
                                .allowIfBaseType(Object.class)
                                .build(),
                        ObjectMapper.DefaultTyping.EVERYTHING,
                        "@class");
    }

    @Override
    public byte[] serialize(Object value) {
        if (value == null) return null;
        try {
            return mapper.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new SagaSerializationException("Failed to serialize value of type "
                    + value.getClass().getName(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] bytes, Class<T> type) {
        if (bytes == null) return null;
        try {
            // @class property in the JSON is authoritative — ignore the `type` hint.
            return (T) mapper.readValue(bytes, Object.class);
        } catch (Exception e) {
            throw new SagaSerializationException("Failed to deserialize bytes", e);
        }
    }

}
