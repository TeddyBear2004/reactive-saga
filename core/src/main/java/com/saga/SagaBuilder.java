package com.saga;

import com.saga.lock.LockableResourceId;
import com.saga.lock.SagaLockService;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Immutable, fluent builder for {@link Saga} instances.
 *
 * @param <I> initial payload type
 * @param <O> final output type
 */
public class SagaBuilder<I, O> {

    private final String name;
    private final List<SagaTransition<?, ?, ?>> transitions;
    private final List<Class<?>> outputHistory;
    private final Class<O> outputClass;
    private final @Nullable SagaLifecycleObserver observer;
    private final @Nullable Function<I, List<LockableResourceId>> lockResourcesExtractor;
    private final @Nullable SagaLockService sagaLockService;

    SagaBuilder(String name, List<SagaTransition<?, ?, ?>> transitions, List<Class<?>> outputHistory,
                Class<O> outputClass, @Nullable SagaLifecycleObserver observer) {
        this(name, transitions, outputHistory, outputClass, observer, null, null);
    }

    SagaBuilder(String name, List<SagaTransition<?, ?, ?>> transitions, List<Class<?>> outputHistory,
                Class<O> outputClass, @Nullable SagaLifecycleObserver observer,
                @Nullable Function<I, List<LockableResourceId>> lockResourcesExtractor,
                @Nullable SagaLockService sagaLockService) {
        this.name = name;
        this.transitions = transitions;
        this.outputHistory = outputHistory;
        this.outputClass = outputClass;
        this.observer = observer;
        this.lockResourcesExtractor = lockResourcesExtractor;
        this.sagaLockService = sagaLockService;
    }

    public SagaBuilder<I, O> withObserver(SagaLifecycleObserver observer) {
        return new SagaBuilder<>(name, transitions, outputHistory, outputClass, observer, lockResourcesExtractor, sagaLockService);
    }

    /**
     * Configures exclusive resource locking around every {@link Saga#execute} call.
     */
    public SagaBuilder<I, O> withLock(Function<I, List<LockableResourceId>> resourcesExtractor) {
        return new SagaBuilder<>(name, transitions, outputHistory, outputClass, observer, resourcesExtractor, sagaLockService);
    }

    /** Sets the {@link SagaLockService}. Normally injected by a factory — not for direct use. */
    public SagaBuilder<I, O> withSagaLockService(@Nullable SagaLockService sagaLockService) {
        return new SagaBuilder<>(name, transitions, outputHistory, outputClass, observer, lockResourcesExtractor, sagaLockService);
    }

    public <NI, NO, L> SagaBuilder<I, O> step(SagaStep<NI, NO, L> step) {
        InputResolver<NI> resolver = createResolver(step.inputType(), outputHistory);
        List<SagaTransition<?, ?, ?>> nextTransitions = append(transitions, new SagaTransition<>(step, resolver));
        List<Class<?>> nextHistory = append(outputHistory, step.outputType());
        return new SagaBuilder<>(name, nextTransitions, nextHistory, outputClass, observer, lockResourcesExtractor, sagaLockService);
    }

    public SagaBuilder<I, O> parallel(String groupName, SagaStep<?, ?, ?>... subSteps) {
        return parallel(groupName, List.of(subSteps));
    }

    public SagaBuilder<I, O> parallel(String groupName, List<SagaStep<?, ?, ?>> subSteps) {
        List<ParallelSagaStepGroup.ParallelMember<?, ?, ?>> members = new ArrayList<>();
        List<Class<?>> nextHistory = new ArrayList<>(outputHistory);
        for (SagaStep<?, ?, ?> s : subSteps) {
            members.add(createParallelMember(s));
            nextHistory.add(s.outputType());
        }
        ParallelSagaStepGroup group = new ParallelSagaStepGroup(groupName, members);
        List<SagaTransition<?, ?, ?>> nextTransitions = append(transitions,
                new SagaTransition<>(group, InputResolvers.parallelGroupResolver()));
        return new SagaBuilder<>(name, nextTransitions, nextHistory, outputClass, observer, lockResourcesExtractor, sagaLockService);
    }

    public SagaBuilder<I, O> injectProperties(Object properties) {
        return step(new InjectPropertyStep<>(properties));
    }

    public Saga<I, O> build() {
        if (transitions.isEmpty()) throw new IllegalStateException("Saga must have at least one step");
        return new Saga<>(name, transitions, outputClass, observer,
                          createResolver(outputClass, outputHistory), lockResourcesExtractor, sagaLockService);
    }

    private <T> ParallelSagaStepGroup.ParallelMember<T, ?, ?> createParallelMember(SagaStep<T, ?, ?> step) {
        return new ParallelSagaStepGroup.ParallelMember<>(step, createResolver(step.inputType(), outputHistory));
    }

    private <T> InputResolver<T> createResolver(Class<T> type, List<Class<?>> history) {
        if (type == Void.class) return InputResolvers.voidResolver();
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
