package hamburg.engelmann.saga;

import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class SagaRetryTest {

    // ── helpers ──────────────────────────────────────────────────────────────

    /**
     * Step that fails {@code failTimes} times before succeeding.
     * Tracks the total number of attempts via {@code attempts}.
     */
    static SagaStep<String, String, String> flakyStep(String name, int failTimes, AtomicInteger attempts) {
        return new SagaStep<>() {
            @Override public String name() { return name; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, String>> execute(String input) {
                int attempt = attempts.incrementAndGet();
                if (attempt <= failTimes) {
                    return Mono.error(new RuntimeException("Attempt " + attempt + " failed"));
                }
                return Mono.just(StepResult.of(input + "-done", input));
            }
            @Override public Mono<Void> compensate(String state) {
                return Mono.empty();
            }
        };
    }

    /** Step that always fails. */
    static SagaStep<String, String, String> alwaysFailingStep(String name) {
        return new SagaStep<>() {
            @Override public String name() { return name; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, String>> execute(String input) {
                return Mono.error(new RuntimeException("always fails"));
            }
            @Override public Mono<Void> compensate(String state) {
                return Mono.empty();
            }
        };
    }

    /** Simple always-succeeding step. */
    static SagaStep<String, String, String> fastStep(String name) {
        return new SagaStep<>() {
            @Override public String name() { return name; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, String>> execute(String input) {
                return Mono.just(StepResult.of(input + "-done", input));
            }
            @Override public Mono<Void> compensate(String state) {
                return Mono.empty();
            }
        };
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void retryable_succeedsOnSecondAttempt() {
        AtomicInteger attempts = new AtomicInteger();

        Saga<String, String> saga = Saga.builder("retry-success", String.class, String.class)
                .step(flakyStep("flaky", 1, attempts))
                .retryable(3)
                .build();

        StepVerifier.create(saga.execute("input"))
                .assertNext(result -> assertThat(result).isEqualTo("input-done"))
                .verifyComplete();

        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void retryable_exhaustedRetries_propagatesError() {
        AtomicInteger attempts = new AtomicInteger();

        Saga<String, String> saga = Saga.builder("retry-fail", String.class, String.class)
                .step(flakyStep("flaky", 5, attempts))
                .retryable(2)  // only 2 retries: 3 attempts total, still not enough
                .build();

        StepVerifier.create(saga.execute("input"))
                .expectError()
                .verify();

        assertThat(attempts.get()).isEqualTo(3); // 1 original + 2 retries
    }

    @Test
    void retryable_onlyAffectsConfiguredStep() {
        AtomicInteger flakyAttempts = new AtomicInteger();

        // first step is retryable; second is not
        Saga<String, String> saga = Saga.builder("retry-selective", String.class, String.class)
                .step(flakyStep("flaky", 1, flakyAttempts))
                .retryable(3)
                .step(fastStep("second"))
                .build();

        StepVerifier.create(saga.execute("input"))
                .assertNext(result -> assertThat(result).isEqualTo("input-done-done"))
                .verifyComplete();

        assertThat(flakyAttempts.get()).isEqualTo(2);
    }

    @Test
    void retryable_withFixedDelay_succeedsAfterRetries() {
        AtomicInteger attempts = new AtomicInteger();

        Saga<String, String> saga = Saga.builder("retry-delay", String.class, String.class)
                .step(flakyStep("flaky", 2, attempts))
                .retryable(3, Duration.ofMillis(10))
                .build();

        StepVerifier.create(saga.execute("input"))
                .assertNext(result -> assertThat(result).isEqualTo("input-done"))
                .verifyComplete();

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void retryable_triggersCompensationAfterExhaustion() {
        List<String> compensated = new ArrayList<>();

        SagaStep<String, String, String> firstStep = new SagaStep<>() {
            @Override public String name() { return "first"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, String>> execute(String input) {
                return Mono.just(StepResult.of(input + "-done", "first-state"));
            }
            @Override public Mono<Void> compensate(String state) {
                compensated.add(state);
                return Mono.empty();
            }
        };

        Saga<String, String> saga = Saga.builder("retry-compensate", String.class, String.class)
                .step(firstStep)
                .step(alwaysFailingStep("second"))
                .retryable(1)
                .build();

        StepVerifier.create(saga.execute("input"))
                .expectError()
                .verify();

        // first step completed and should be compensated after second step exhausts retries
        assertThat(compensated).contains("first-state");
    }

    @Test
    void retryable_zeroRetries_behavesLikeNoRetry() {
        AtomicInteger attempts = new AtomicInteger();

        Saga<String, String> saga = Saga.builder("retry-zero", String.class, String.class)
                .step(flakyStep("flaky", 1, attempts))
                .retryable(0)
                .build();

        StepVerifier.create(saga.execute("input"))
                .expectError()
                .verify();

        assertThat(attempts.get()).isEqualTo(1);
    }
}
