package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.di.internal.InputResolver;
import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Executes multiple saga steps in parallel. On failure, compensates only the steps
 * that completed successfully before the error occurred.
 */
public class ParallelSagaStepGroup implements SagaStep<Object, ParallelOutputs, Map<String, Object>> {

    private static final Logger log = LoggerFactory.getLogger(ParallelSagaStepGroup.class);

    private final String name;
    private final List<ParallelMember<?, ?, ?>> members;

    public ParallelSagaStepGroup(String name, List<ParallelMember<?, ?, ?>> members) {
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("Members list must not be empty");
        }
        this.name = name;
        this.members = List.copyOf(members);
    }

    @Override public String name() { return name; }
    @Override public Class<Object> inputType() { return Object.class; }
    @Override public Class<ParallelOutputs> outputType() { return ParallelOutputs.class; }

    @Override
    @SuppressWarnings("unchecked")
    public Mono<StepResult<ParallelOutputs, Map<String, Object>>> execute(Object context) {
        List<Object> history = (List<Object>) context;
        Map<String, Object> localStates = new ConcurrentHashMap<>();
        List<ParallelMember<?, ?, ?>> completedMembers = new CopyOnWriteArrayList<>();

        return Flux.fromIterable(members)
                .flatMapSequential(member -> {
                    log.debug("[ParallelSagaStepGroup:{}] Starting sub-step: {}", name, member.step().name());
                    return member.executeWithInjection(history)
                            .doOnSuccess(result -> {
                                if (result != null && result.localState() != null) {
                                    localStates.put(member.step().name(), result.localState());
                                }
                                completedMembers.add(member);
                            })
                            .map(result -> result.output() != null ? result.output() : new Object());
                })
                .collectList()
                .map(outputs -> new StepResult<>(new ParallelOutputs(outputs), localStates))
                .onErrorResume(error -> compensatePartial(completedMembers, localStates, error));
    }

    @Override
    public Mono<Void> compensate(Map<String, Object> localStates) {
        Map<String, Object> states = Optional.ofNullable(localStates).orElse(Collections.emptyMap());
        log.debug("[ParallelSagaStepGroup:{}] Orchestrator triggered rollback. Compensating {} steps", name, members.size());
        return executeParallelCompensation(members, states);
    }

    private Mono<StepResult<ParallelOutputs, Map<String, Object>>> compensatePartial(
            List<ParallelMember<?, ?, ?>> completed, Map<String, Object> states, Throwable error) {
        log.error("[ParallelSagaStepGroup:{}] Execution failed. Rolling back {} completed steps. Cause: {}",
                  name, completed.size(), error.getMessage());
        return executeParallelCompensation(completed, states).then(Mono.error(error));
    }

    @SuppressWarnings("unchecked")
    private Mono<Void> executeParallelCompensation(
            Iterable<? extends ParallelMember<?, ?, ?>> toCompensate, Map<String, Object> states) {
        return Flux.fromIterable(toCompensate)
                .concatMap(member -> {
                    log.debug("[ParallelSagaStepGroup:{}] Compensating step: {}", name, member.step().name());
                    return ((SagaStep<Object, Object, Object>) member.step())
                            .compensate(states.get(member.step().name()))
                            .onErrorResume(err -> {
                                log.error("[ParallelSagaStepGroup:{}] Compensation failed for: {}. Error: {}",
                                          name, member.step().name(), err.getMessage());
                                return Mono.empty();
                            });
                })
                .then();
    }

    public record ParallelMember<I, O, L>(SagaStep<I, O, L> step, InputResolver<I> inputResolver) {
        public Mono<StepResult<O, L>> executeWithInjection(List<Object> history) {
            return step.execute(inputResolver.resolve(null, history));
        }
    }
}
