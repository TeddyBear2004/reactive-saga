package com.saga.internal;

public class InputResolvers {
    public static <I> InputResolver<I> voidResolver() {
        return (raw, _) -> null;
    }

    @SuppressWarnings("unchecked")
    public static <I> InputResolver<I> parallelGroupResolver() {
        return (raw, outputs) -> (I) outputs;
    }

    @SuppressWarnings("unchecked")
    public static <I> InputResolver<I> directResolver() {
        return (raw, outputs) -> (I) raw;
    }

    @SuppressWarnings("unchecked")
    public static <I> InputResolver<I> proxyResolver(SagaProxyFactory factory) {
        return (raw, outputs) -> (I) factory.createInput(outputs);
    }

}
