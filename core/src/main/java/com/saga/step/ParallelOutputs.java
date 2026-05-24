package com.saga.step;

import java.util.List;

/**
 * Aggregated output of all sub-steps in a parallel group.
 * The outputs are in the same order as the sub-steps were declared.
 */
public record ParallelOutputs(List<Object> values) {}
