package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import reactor.core.publisher.Mono;

/**
 * A saga step whose only purpose is to inject static properties or configuration
 * into the output history so subsequent steps can receive them via proxy injection.
 *
 * @param <P> the properties type (record or interface)
 */
public class InjectPropertyStep<P> implements SagaStep<Void, P, Void> {

    private final String name;
    private final P properties;

    public InjectPropertyStep(P properties) {
        this.name = "InjectProperties[" + properties.getClass().getSimpleName() + "]";
        this.properties = properties;
    }

    public InjectPropertyStep(String name, P properties) {
        this.name = name;
        this.properties = properties;
    }

    @Override public String name() { return name; }
    @Override public Class<Void> inputType() { return Void.class; }

    @Override
    @SuppressWarnings("unchecked")
    public Class<P> outputType() { return (Class<P>) properties.getClass(); }

    @Override
    public Mono<StepResult<P, Void>> execute(Void input) {
        return Mono.just(StepResult.stateless(properties));
    }

}
