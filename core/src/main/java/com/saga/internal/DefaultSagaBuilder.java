package com.saga.internal;

import com.saga.Saga;
import com.saga.SagaBuilder;
import com.saga.SagaPersistenceHook;
import com.saga.SagaStepBuilder;
import com.saga.lifecycle.SagaLifecycleObserver;
import com.saga.lock.LockableResourceId;
import com.saga.lock.SagaLockService;
import com.saga.step.SagaStep;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Immutable, fluent implementation of {@link SagaBuilder}.
 * Consumers obtain instances via {@link Saga#builder(String, Class, Class)}.
 */
public class DefaultSagaBuilder<I, O> implements SagaBuilder<I, O> {

    private final String name;
    private final List<SagaTransition<?, ?, ?>> transitions;
    private final List<Class<?>> outputHistory;
    private final Class<O> outputClass;
    private final @Nullable SagaLifecycleObserver observer;
    private final @Nullable Function<I, List<LockableResourceId>> lockResourcesExtractor;
    private final @Nullable SagaLockService sagaLockService;
    private final @Nullable Function<I, String> correlationIdExtractor;
    private final @Nullable SagaPersistenceHook persistenceHook;
    private final @Nullable Duration sagaTimeout;

    public DefaultSagaBuilder(String name, List<SagaTransition<?, ?, ?>> transitions,
                               List<Class<?>> outputHistory, Class<O> outputClass,
                               @Nullable SagaLifecycleObserver observer) {
        this(name, transitions, outputHistory, outputClass, observer, null, null, null, null, null);
    }

    public DefaultSagaBuilder(String name, List<SagaTransition<?, ?, ?>> transitions,
                               List<Class<?>> outputHistory, Class<O> outputClass,
                               @Nullable SagaLifecycleObserver observer,
                               @Nullable Function<I, List<LockableResourceId>> lockResourcesExtractor,
                               @Nullable SagaLockService sagaLockService,
                               @Nullable Function<I, String> correlationIdExtractor,
                               @Nullable SagaPersistenceHook persistenceHook,
                               @Nullable Duration sagaTimeout) {
        this.name = name;
        this.transitions = transitions;
        this.outputHistory = outputHistory;
        this.outputClass = outputClass;
        this.observer = observer;
        this.lockResourcesExtractor = lockResourcesExtractor;
        this.sagaLockService = sagaLockService;
        this.correlationIdExtractor = correlationIdExtractor;
        this.persistenceHook = persistenceHook;
        this.sagaTimeout = sagaTimeout;
    }

    @Override
    public SagaBuilder<I, O> withObserver(SagaLifecycleObserver observer) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, observer,
                                        lockResourcesExtractor, sagaLockService, correlationIdExtractor, persistenceHook, sagaTimeout);
    }

    @Override
    public SagaBuilder<I, O> withLock(Function<I, List<LockableResourceId>> resourcesExtractor) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, observer,
                                        resourcesExtractor, sagaLockService, correlationIdExtractor, persistenceHook, sagaTimeout);
    }

    @Override
    public SagaBuilder<I, O> withSagaLockService(@Nullable SagaLockService sagaLockService) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, observer,
                                        lockResourcesExtractor, sagaLockService, correlationIdExtractor, persistenceHook, sagaTimeout);
    }

    @Override
    public SagaBuilder<I, O> withCorrelationId(Function<I, String> extractor) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, observer,
                                        lockResourcesExtractor, sagaLockService, extractor, persistenceHook, sagaTimeout);
    }

    @Override
    public SagaBuilder<I, O> withPersistenceHook(SagaPersistenceHook hook) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, observer,
                                        lockResourcesExtractor, sagaLockService, correlationIdExtractor, hook, sagaTimeout);
    }

    @Override
    public SagaBuilder<I, O> timeout(Duration duration) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, observer,
                                        lockResourcesExtractor, sagaLockService, correlationIdExtractor, persistenceHook, duration);
    }

    /** Returns a new builder with the timeout applied to the most recently added transition. */
    DefaultSagaBuilder<I, O> withLastTransitionTimeout(Duration timeout) {
        List<SagaTransition<?, ?, ?>> updated = new ArrayList<>(transitions);
        SagaTransition<?, ?, ?> last = updated.removeLast();
        updated.add(last.withTimeout(timeout));
        return new DefaultSagaBuilder<>(name, updated, outputHistory, outputClass, observer,
                                        lockResourcesExtractor, sagaLockService, correlationIdExtractor, persistenceHook, sagaTimeout);
    }

    @Override
    public <NI, NO, L> SagaStepBuilder<I, O> step(SagaStep<NI, NO, L> step) {
        InputResolver<NI> resolver = createResolver(step.inputType(), outputHistory);
        List<SagaTransition<?, ?, ?>> nextTransitions = append(transitions, new SagaTransition<>(step, resolver));
        List<Class<?>> nextHistory = append(outputHistory, step.outputType());
        DefaultSagaBuilder<I, O> next = new DefaultSagaBuilder<>(name, nextTransitions, nextHistory, outputClass, observer,
                                                                   lockResourcesExtractor, sagaLockService, correlationIdExtractor, persistenceHook, sagaTimeout);
        return new DefaultSagaStepBuilder<>(next);
    }

    @Override
    public SagaStepBuilder<I, O> parallel(String groupName, SagaStep<?, ?, ?>... subSteps) {
        return parallel(groupName, List.of(subSteps));
    }

    @Override
    public SagaStepBuilder<I, O> parallel(String groupName, List<SagaStep<?, ?, ?>> subSteps) {
        List<ParallelSagaStepGroup.ParallelMember<?, ?, ?>> members = new ArrayList<>();
        List<Class<?>> nextHistory = new ArrayList<>(outputHistory);
        for (SagaStep<?, ?, ?> s : subSteps) {
            members.add(createParallelMember(s));
            nextHistory.add(s.outputType());
        }
        ParallelSagaStepGroup group = new ParallelSagaStepGroup(groupName, members);
        List<SagaTransition<?, ?, ?>> nextTransitions = append(transitions,
                new SagaTransition<>(group, InputResolvers.parallelGroupResolver()));
        DefaultSagaBuilder<I, O> next = new DefaultSagaBuilder<>(name, nextTransitions, nextHistory, outputClass, observer,
                                                                   lockResourcesExtractor, sagaLockService, correlationIdExtractor, persistenceHook, sagaTimeout);
        return new DefaultSagaStepBuilder<>(next);
    }

    @Override
    public SagaBuilder<I, O> injectProperties(Object properties) {
        return step(new InjectPropertyStep<>(properties));
    }

    @Override
    public Saga<I, O> build() {
        if (transitions.isEmpty()) throw new IllegalStateException("Saga must have at least one step");
        return new SagaImpl<>(name, transitions, outputClass, observer,
                              createResolver(outputClass, outputHistory), lockResourcesExtractor, sagaLockService,
                              correlationIdExtractor, persistenceHook, sagaTimeout);
    }

    private <T> ParallelSagaStepGroup.ParallelMember<T, ?, ?> createParallelMember(SagaStep<T, ?, ?> step) {
        return new ParallelSagaStepGroup.ParallelMember<>(step, createResolver(step.inputType(), outputHistory));
    }

    private <T> InputResolver<T> createResolver(Class<T> type, List<Class<?>> history) {
        if (type == Void.class) return InputResolvers.voidResolver();
        if (!history.isEmpty() && history.getLast() == type) return InputResolvers.directResolver();
        if (type.isInterface() || type.isRecord()) {
            return InputResolvers.proxyResolver(new SagaProxyFactory(type, SagaInputMapper.buildMapping(type, history)));
        }
        return InputResolvers.directResolver();
    }

    private static <T> List<T> append(List<T> list, T element) {
        List<T> next = new ArrayList<>(list);
        next.add(element);
        return next;
    }
}
