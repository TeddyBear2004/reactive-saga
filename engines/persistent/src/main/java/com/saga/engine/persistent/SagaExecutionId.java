package com.saga.engine.persistent;

/**
 * Uniquely identifies a single saga execution instance.
 *
 * @param sagaName      the saga's logical name (from {@code Saga.builder(name, ...)})
 * @param correlationId caller-provided identifier correlating this run to a business entity
 *                      (e.g. order ID, payment ID)
 */
public record SagaExecutionId(String sagaName, String correlationId) {

    public static SagaExecutionId of(String sagaName, String correlationId) {
        return new SagaExecutionId(sagaName, correlationId);
    }

}
