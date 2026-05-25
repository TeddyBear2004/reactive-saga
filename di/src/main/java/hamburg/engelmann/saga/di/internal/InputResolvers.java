package hamburg.engelmann.saga.di.internal;

public class InputResolvers {
    public static <I> InputResolver<I> voidResolver() {
        return (_, _) -> null;
    }

    @SuppressWarnings("unchecked")
    public static <I> InputResolver<I> parallelGroupResolver() {
        return (_, outputs) -> (I) outputs;
    }

    @SuppressWarnings("unchecked")
    public static <I> InputResolver<I> directResolver() {
        return (raw, _) -> (I) raw;
    }

    @SuppressWarnings("unchecked")
    public static <I> InputResolver<I> proxyResolver(SagaProxyFactory factory) {
        return (_, outputs) -> (I) factory.createInput(outputs);
    }

}
