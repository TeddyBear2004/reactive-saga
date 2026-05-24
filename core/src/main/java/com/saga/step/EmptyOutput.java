package com.saga.step;

/** Sentinel type used when a step produces no meaningful output. */
public final class EmptyOutput {

    EmptyOutput() {}

    @Override public String toString() { return "EmptyOutput"; }

}
