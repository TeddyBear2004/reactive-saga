package com.saga;

class InputResolvers {

    static <I> InputResolver<I> voidResolver() {
        return (raw, _) -> null;
    }

    @SuppressWarnings("unchecked")
    static <I> InputResolver<I> parallelGroupResolver() {
        return (raw, outputs) -> (I) outputs;
    }

    @SuppressWarnings("unchecked")
    static <I> InputResolver<I> directResolver() {
        return (raw, outputs) -> (I) raw;
    }

    @SuppressWarnings("unchecked")
    static <I> InputResolver<I> proxyResolver(SagaProxyFactory factory) {
        return (raw, outputs) -> (I) factory.createInput(outputs);
    }

}
