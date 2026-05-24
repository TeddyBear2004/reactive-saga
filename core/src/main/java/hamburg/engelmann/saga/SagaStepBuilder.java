package hamburg.engelmann.saga;

import java.time.Duration;

/**
 * Fluent builder returned by {@link SagaBuilder#step} and {@link SagaBuilder#parallel}.
 * Extends {@link SagaBuilder} with the ability to set a timeout or retry policy for the
 * most recently added step.
 *
 * <p>Example:
 * <pre>{@code
 * Saga.builder("Flow", Input.class, Output.class)
 *         .step(new ValidateStep())
 *         .timeout(Duration.ofSeconds(5))   // applies only to ValidateStep
 *         .step(new ProcessStep())
 *         .retryable(3)                      // retry ProcessStep up to 3 times
 *         .build();
 * }</pre>
 *
 * @param <I> initial payload type
 * @param <O> final output type
 */
public interface SagaStepBuilder<I, O> extends SagaBuilder<I, O> {

    /**
     * Sets a timeout for the most recently added step.
     * If the step does not complete within this duration, a {@link java.util.concurrent.TimeoutException}
     * is propagated and saga compensation is triggered.
     *
     * @param stepTimeout maximum duration for this step
     * @return a {@link SagaBuilder} to continue configuring the saga
     */
    @Override
    SagaBuilder<I, O> timeout(Duration stepTimeout);

    /**
     * Retries the most recently added step up to {@code maxAttempts} additional times
     * (i.e. the step is attempted at most {@code maxAttempts + 1} times in total)
     * without any delay between attempts.
     *
     * @param maxAttempts number of retries (must be &gt;= 0)
     * @return this builder to allow further step configuration
     */
    SagaStepBuilder<I, O> retryable(int maxAttempts);

    /**
     * Retries the most recently added step up to {@code maxAttempts} additional times
     * with a fixed delay between each attempt.
     *
     * @param maxAttempts number of retries (must be &gt;= 0)
     * @param fixedDelay  delay to wait before each retry attempt
     * @return this builder to allow further step configuration
     */
    SagaStepBuilder<I, O> retryable(int maxAttempts, Duration fixedDelay);
}
