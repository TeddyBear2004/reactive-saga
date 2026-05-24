package com.saga.internal;

import java.util.List;

/**
 * Strategy interface for resolving the input fed into a saga step.
 */
@FunctionalInterface
public interface InputResolver<I> {

    I resolve(Object rawInput, List<Object> executionOutputs);

}
