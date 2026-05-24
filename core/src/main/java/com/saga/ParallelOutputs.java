package com.saga;

import java.util.List;

/**
 * Wraps the outputs of a parallel step group so the execution context
 * can unpack them into individual history entries.
 */
public record ParallelOutputs(List<Object> values) { }
