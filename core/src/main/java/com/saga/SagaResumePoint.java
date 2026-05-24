package com.saga;

import java.util.List;

/**
 * Describes the persisted state of a saga execution that can be resumed after a crash.
 *
 * @param completedTransitions number of transitions (steps / parallel groups) already completed;
 *                             used as the resume index in the step chain
 * @param preloadedOutputs     flat ordered list of all step outputs collected so far
 *                             (a parallel group contributes N entries — one per parallel member)
 */
public record SagaResumePoint(int completedTransitions, List<Object> preloadedOutputs) {

    public static SagaResumePoint of(int completedTransitions, List<Object> preloadedOutputs) {
        return new SagaResumePoint(completedTransitions, preloadedOutputs);
    }

}
