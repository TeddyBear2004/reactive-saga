package com.saga;

import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.lang.reflect.RecordComponent;
import java.util.List;
import java.util.Map;

/**
 * Creates step input instances from the mapping produced by {@link SagaInputMapper}.
 */
class SagaProxyFactory {

    private final Class<?> inputType;
    private final Map<String, SagaInputMapper.OutputLocator> mapping;

    SagaProxyFactory(Class<?> inputType, Map<String, SagaInputMapper.OutputLocator> mapping) {
        this.inputType = inputType;
        this.mapping = mapping;
    }

    Object createInput(List<Object> outputs) {
        if (inputType.isInterface()) return createProxy(outputs);
        if (inputType.isRecord()) return createRecord(outputs);
        throw new IllegalStateException("Input type must be interface or record: " + inputType.getName());
    }

    private Object createProxy(List<Object> outputs) {
        return Proxy.newProxyInstance(
                inputType.getClassLoader(),
                new Class[]{inputType},
                (_, method, _) -> {
                    SagaInputMapper.OutputLocator loc = mapping.get(method.getName());
                    Object target = outputs.get(loc.stepIndex());
                    return loc.accessor() == null ? target : loc.accessor().invoke(target);
                }
        );
    }

    private Object createRecord(List<Object> outputs) {
        try {
            RecordComponent[] components = inputType.getRecordComponents();
            Object[] args = new Object[components.length];
            Class<?>[] types = new Class<?>[components.length];
            for (int i = 0; i < components.length; i++) {
                SagaInputMapper.OutputLocator loc = mapping.get(components[i].getName());
                Object target = outputs.get(loc.stepIndex());
                args[i] = loc.accessor() == null ? target : loc.accessor().invoke(target);
                types[i] = components[i].getType();
            }
            Constructor<?> ctor = inputType.getDeclaredConstructor(types);
            return ctor.newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate record: " + inputType.getName(), e);
        }
    }
}
