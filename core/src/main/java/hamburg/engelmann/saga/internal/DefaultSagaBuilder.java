package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.Saga;
import hamburg.engelmann.saga.SagaBuilder;
import hamburg.engelmann.saga.SagaPersistenceHook;
import hamburg.engelmann.saga.SagaStepBuilder;
import hamburg.engelmann.saga.lifecycle.SagaLifecycleObserver;
import hamburg.engelmann.saga.lock.LockableResourceId;
import hamburg.engelmann.saga.lock.SagaLockService;
import hamburg.engelmann.saga.step.SagaStep;
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
    private final SagaConfig<I, O> config;

    /** Entry-point constructor used by {@link Saga#builder}. */
    public DefaultSagaBuilder(String name, List<SagaTransition<?, ?, ?>> transitions,
                               List<Class<?>> outputHistory, Class<O> outputClass,
                               @Nullable SagaLifecycleObserver observer) {
        this(name, transitions, outputHistory, outputClass, new SagaConfig<>(observer, null, null, null, null, null));
    }

    private DefaultSagaBuilder(String name, List<SagaTransition<?, ?, ?>> transitions,
                                List<Class<?>> outputHistory, Class<O> outputClass,
                                SagaConfig<I, O> config) {
        this.name = name;
        this.transitions = transitions;
        this.outputHistory = outputHistory;
        this.outputClass = outputClass;
        this.config = config;
    }

    /** Bundles all optional saga-level configuration to avoid a 10-parameter constructor. */
    private record SagaConfig<I, O>(
            @Nullable SagaLifecycleObserver observer,
            @Nullable Function<I, List<LockableResourceId>> lockResourcesExtractor,
            @Nullable SagaLockService sagaLockService,
            @Nullable Function<I, String> correlationIdExtractor,
            @Nullable SagaPersistenceHook persistenceHook,
            @Nullable Duration sagaTimeout) {

        SagaConfig<I, O> withObserver(@Nullable SagaLifecycleObserver obs) {
            return new SagaConfig<>(obs, lockResourcesExtractor, sagaLockService, correlationIdExtractor, persistenceHook, sagaTimeout);
        }

        SagaConfig<I, O> withLock(Function<I, List<LockableResourceId>> extractor) {
            return new SagaConfig<>(observer, extractor, sagaLockService, correlationIdExtractor, persistenceHook, sagaTimeout);
        }

        SagaConfig<I, O> withSagaLockService(@Nullable SagaLockService svc) {
            return new SagaConfig<>(observer, lockResourcesExtractor, svc, correlationIdExtractor, persistenceHook, sagaTimeout);
        }

        SagaConfig<I, O> withCorrelationId(Function<I, String> extractor) {
            return new SagaConfig<>(observer, lockResourcesExtractor, sagaLockService, extractor, persistenceHook, sagaTimeout);
        }

        SagaConfig<I, O> withPersistenceHook(SagaPersistenceHook hook) {
            return new SagaConfig<>(observer, lockResourcesExtractor, sagaLockService, correlationIdExtractor, hook, sagaTimeout);
        }

        SagaConfig<I, O> withTimeout(Duration duration) {
            return new SagaConfig<>(observer, lockResourcesExtractor, sagaLockService, correlationIdExtractor, persistenceHook, duration);
        }
    }

    @Override
    public SagaBuilder<I, O> withObserver(SagaLifecycleObserver observer) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, config.withObserver(observer));
    }

    @Override
    public SagaBuilder<I, O> withLock(Function<I, List<LockableResourceId>> resourcesExtractor) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, config.withLock(resourcesExtractor));
    }

    @Override
    public SagaBuilder<I, O> withSagaLockService(@Nullable SagaLockService sagaLockService) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, config.withSagaLockService(sagaLockService));
    }

    @Override
    public SagaBuilder<I, O> withCorrelationId(Function<I, String> extractor) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, config.withCorrelationId(extractor));
    }

    @Override
    public SagaBuilder<I, O> withPersistenceHook(SagaPersistenceHook hook) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, config.withPersistenceHook(hook));
    }

    @Override
    public SagaBuilder<I, O> timeout(Duration duration) {
        return new DefaultSagaBuilder<>(name, transitions, outputHistory, outputClass, config.withTimeout(duration));
    }

    /** Returns a new builder with the timeout applied to the most recently added transition. */
    DefaultSagaBuilder<I, O> withLastTransitionTimeout(Duration timeout) {
        List<SagaTransition<?, ?, ?>> updated = new ArrayList<>(transitions);
        SagaTransition<?, ?, ?> last = updated.removeLast();
        updated.add(last.withTimeout(timeout));
        return new DefaultSagaBuilder<>(name, updated, outputHistory, outputClass, config);
    }

    /** Returns a new builder with the retry spec applied to the most recently added transition. */
    DefaultSagaBuilder<I, O> withLastTransitionRetry(reactor.util.retry.Retry retrySpec) {
        List<SagaTransition<?, ?, ?>> updated = new ArrayList<>(transitions);
        SagaTransition<?, ?, ?> last = updated.removeLast();
        updated.add(last.withRetry(retrySpec));
        return new DefaultSagaBuilder<>(name, updated, outputHistory, outputClass, config);
    }

    @Override
    public <NI, NO, L> SagaStepBuilder<I, O> step(SagaStep<NI, NO, L> step) {
        InputResolver<NI> resolver = createResolver(step.inputType(), outputHistory);
        List<SagaTransition<?, ?, ?>> nextTransitions = append(transitions, new SagaTransition<>(step, resolver));
        List<Class<?>> nextHistory = append(outputHistory, step.outputType());
        return new DefaultSagaStepBuilder<>(new DefaultSagaBuilder<>(name, nextTransitions, nextHistory, outputClass, config));
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
        return new DefaultSagaStepBuilder<>(new DefaultSagaBuilder<>(name, nextTransitions, nextHistory, outputClass, config));
    }

    @Override
    public SagaBuilder<I, O> injectProperties(Object properties) {
        return step(new InjectPropertyStep<>(properties));
    }

    @Override
    public Saga<I, O> build() {
        if (transitions.isEmpty()) throw new IllegalStateException("Saga must have at least one step");
        return new SagaImpl<>(name, transitions, outputClass, config.observer(),
                              createResolver(outputClass, outputHistory), config.lockResourcesExtractor(),
                              config.sagaLockService(), config.correlationIdExtractor(),
                              config.persistenceHook(), config.sagaTimeout());
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
