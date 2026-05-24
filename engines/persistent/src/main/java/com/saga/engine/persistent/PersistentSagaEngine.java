package com.saga.engine.persistent;

import com.saga.Saga;
import com.saga.SagaBuilder;
import com.saga.SagaEngine;
import com.saga.SagaPersistenceHook;
import com.saga.SagaResumePoint;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.SagaLockService;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link SagaEngine} that persists execution state after every completed step,
 * enabling crash recovery and durable distributed saga execution.
 *
 * <p>On each step completion the serialized output(s) are appended to
 * {@link SagaExecutionState} and saved via {@link SagaExecutionRepository}.
 * On the next execution with the same {@code correlationId}, persisted outputs are loaded and
 * completed steps are skipped — execution resumes from the first incomplete step.
 *
 * <p>Usage:
 * <pre>{@code
 * SagaEngine engine = new PersistentSagaEngine(repository,
 *         SagaStateSerializer.jacksonWithFqcn(objectMapper));
 *
 * Saga<Order, Receipt> saga = engine.build(
 *         Saga.builder("OrderFlow", Order.class, Receipt.class)
 *                 .withCorrelationId(Order::orderId)
 *                 .step(new ValidateOrderStep())
 *                 .step(new ReserveInventoryStep())
 * );
 * }</pre>
 *
 * @see SagaExecutionRepository
 * @see SagaStateSerializer
 */
public final class PersistentSagaEngine implements SagaEngine {

    private final SagaExecutionRepository repository;
    private final SagaStateSerializer serializer;
    private final @Nullable SagaLifecycleObserver observer;
    private final @Nullable SagaLockService lockService;

    public PersistentSagaEngine(SagaExecutionRepository repository, SagaStateSerializer serializer) {
        this(repository, serializer, null, null);
    }

    private PersistentSagaEngine(SagaExecutionRepository repository,
                                  SagaStateSerializer serializer,
                                  @Nullable SagaLifecycleObserver observer,
                                  @Nullable SagaLockService lockService) {
        this.repository = repository;
        this.serializer = serializer;
        this.observer = observer;
        this.lockService = lockService;
    }

    @Override
    public SagaEngine withObserver(SagaLifecycleObserver observer) {
        return new PersistentSagaEngine(repository, serializer, observer, lockService);
    }

    @Override
    public SagaEngine withLockService(SagaLockService lockService) {
        return new PersistentSagaEngine(repository, serializer, observer, lockService);
    }

    @Override
    public <I, O> Saga<I, O> build(SagaBuilder<I, O> builder) {
        SagaBuilder<I, O> configured = builder;
        if (observer != null) configured = configured.withObserver(observer);
        if (lockService != null) configured = configured.withSagaLockService(lockService);
        return configured
                .withPersistenceHook(new PersistenceHook())
                .build();
    }

    private final class PersistenceHook implements SagaPersistenceHook {

        @Override
        public Mono<Void> afterStep(String sagaName, String correlationId,
                                    int transitionIndex, List<Object> stepOutputs) {
            SagaExecutionId id = new SagaExecutionId(sagaName, correlationId);
            return repository.findById(id)
                    .defaultIfEmpty(newState(id))
                    .flatMap(state -> {
                        List<byte[]> updated = new ArrayList<>(state.serializedStepOutputs());
                        for (Object output : stepOutputs) {
                            updated.add(serializer.serialize(output));
                        }
                        return repository.save(new SagaExecutionState(
                                state.id(),
                                SagaExecutionStatus.IN_PROGRESS,
                                transitionIndex + 1,
                                Collections.unmodifiableList(updated),
                                state.createdAt(),
                                Instant.now()
                        ));
                    })
                    .then();
        }

        @Override
        public Mono<SagaResumePoint> loadResumePoint(String sagaName, String correlationId) {
            return repository.findById(new SagaExecutionId(sagaName, correlationId))
                    .filter(s -> s.status() == SagaExecutionStatus.IN_PROGRESS)
                    .map(state -> {
                        List<Object> outputs = new ArrayList<>();
                        for (byte[] bytes : state.serializedStepOutputs()) {
                            outputs.add(serializer.deserialize(bytes, Object.class));
                        }
                        return SagaResumePoint.of(state.currentStepIndex(),
                                                  Collections.unmodifiableList(outputs));
                    });
        }

        @Override
        public Mono<Void> onCompleted(String sagaName, String correlationId) {
            return updateStatus(sagaName, correlationId, SagaExecutionStatus.COMPLETED);
        }

        @Override
        public Mono<Void> onFailed(String sagaName, String correlationId) {
            return updateStatus(sagaName, correlationId, SagaExecutionStatus.FAILED);
        }

        private Mono<Void> updateStatus(String sagaName, String correlationId,
                                        SagaExecutionStatus status) {
            SagaExecutionId id = new SagaExecutionId(sagaName, correlationId);
            return repository.findById(id)
                    .flatMap(state -> repository.save(new SagaExecutionState(
                            state.id(),
                            status,
                            state.currentStepIndex(),
                            state.serializedStepOutputs(),
                            state.createdAt(),
                            Instant.now()
                    )))
                    .then();
        }

        private SagaExecutionState newState(SagaExecutionId id) {
            Instant now = Instant.now();
            return new SagaExecutionState(id, SagaExecutionStatus.IN_PROGRESS, 0,
                                          Collections.emptyList(), now, now);
        }

    }

}
