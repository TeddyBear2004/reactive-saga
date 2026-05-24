package hamburg.engelmann.saga.spring;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Type-safe configuration properties for the saga engine.
 *
 * <p>Example {@code application.yml}:
 * <pre>{@code
 * saga:
 *   observer:
 *     logging-enabled: false
 * }</pre>
 */
@Getter
@ConfigurationProperties(prefix = "saga")
public class SagaProperties {

    private final Observer observer = new Observer();

    @Setter
    @Getter
    public static class Observer {

        /** Whether the built-in logging observer is registered. Defaults to {@code true}. */
        private boolean loggingEnabled = true;

    }
}
