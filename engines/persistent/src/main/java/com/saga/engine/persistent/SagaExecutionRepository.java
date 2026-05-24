package com.saga.engine.persistent;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * SPI for persisting and retrieving saga execution state.
 *
 * <p>Implement this interface for your datastore of choice (R2DBC, MongoDB, Redis, …).
 * The {@link PersistentSagaEngine} calls it after every completed step so that
 * in-progress executions can be recovered after a crash.
 */
public interface SagaExecutionRepository {

    /** Inserts or updates the execution state. */
    Mono<SagaExecutionState> save(SagaExecutionState state);

    /** Returns the execution state for the given ID, or empty if not found. */
    Mono<SagaExecutionState> findById(SagaExecutionId id);

    /** Returns all executions that are currently {@link SagaExecutionStatus#IN_PROGRESS}. */
    Flux<SagaExecutionState> findAllInProgress();

    /** Removes the persisted state for a completed or compensated execution. */
    Mono<Void> deleteById(SagaExecutionId id);

}
