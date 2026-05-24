package hamburg.engelmann.saga.internal;

import java.util.List;

/**
 * Aggregated output of all sub-steps in a parallel group.
 * This is an internal type — consumers never reference it directly.
 */
public record ParallelOutputs(List<Object> values) {}
