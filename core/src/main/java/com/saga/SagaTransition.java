package com.saga;

import reactor.core.publisher.Mono;

import java.util.List;

class SagaTransition<I, O, L> {

    private final SagaStep<I, O, L> step;
    private final InputResolver<I> inputResolver;

    SagaTransition(SagaStep<I, O, L> step, InputResolver<I> inputResolver) {
        this.step = step;
        this.inputResolver = inputResolver;
    }

    String stepName() { return step.name(); }

    Mono<StepResult<Object, Object>> executeValidated(Object rawInput, List<Object> executionOutputs) {
        Object actualInput = inputResolver.resolve(rawInput, executionOutputs);
        Class<I> expectedType = step.inputType();

        if (actualInput != null && !expectedType.isInstance(actualInput)) {
            return Mono.error(new IllegalArgumentException(
                    String.format("[Saga Transition Error] Step '%s' expects '%s' but got '%s'",
                                  step.name(), expectedType.getSimpleName(), actualInput.getClass().getSimpleName())
            ));
        }

        return step.execute(expectedType.cast(actualInput))
                .map(res -> new StepResult<>(res.output(), res.localState()));
    }

    @SuppressWarnings("unchecked")
    Mono<Void> compensateValidated(Object localState) {
        return step.compensate((L) localState);
    }
}
