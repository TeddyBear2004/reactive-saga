package com.saga;

import com.saga.step.SagaStep;
import com.saga.step.StepResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class SagaTimeoutTest {

    // ── helpers ──────────────────────────────────────────────────────────────

    /** String → String step that completes instantly, appending "-done". */
    static SagaStep<String, String, String> fastStep(String name, List<String> compensated) {
        return new SagaStep<>() {
            @Override public String name() { return name; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, String>> execute(String input) {
                return Mono.just(StepResult.of(input + "-done", input));
            }
            @Override public Mono<Void> compensate(String state) {
                compensated.add(state);
                return Mono.empty();
            }
        };
    }

    /** String → String step that delays by the given duration before completing. */
    static SagaStep<String, String, String> slowStep(String name, Duration delay, List<String> compensated) {
        return new SagaStep<>() {
            @Override public String name() { return name; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<String> outputType() { return String.class; }
            @Override public Mono<StepResult<String, String>> execute(String input) {
                return Mono.delay(delay)
                           .map(_ -> StepResult.of(input + "-done", input));
            }
            @Override public Mono<Void> compensate(String state) {
                compensated.add(state);
                return Mono.empty();
            }
        };
    }

    // ── step-level timeout tests ──────────────────────────────────────────────

    @Test
    void stepTimeout_slowStep_timesOut() {
        Saga<String, String> saga = Saga.builder("step-timeout", String.class, String.class)
                .step(slowStep("slow", Duration.ofSeconds(10), new ArrayList<>()))
                .timeout(Duration.ofMillis(50))
                .build();

        StepVerifier.withVirtualTime(() -> saga.execute("input"))
                .thenAwait(Duration.ofMillis(100))
                .expectError(TimeoutException.class)
                .verify();
    }

    @Test
    void stepTimeout_fastStep_completesBeforeTimeout() {
        Saga<String, String> saga = Saga.builder("step-fast", String.class, String.class)
                .step(fastStep("fast", new ArrayList<>()))
                .timeout(Duration.ofSeconds(10))
                .build();

        StepVerifier.create(saga.execute("input"))
                .assertNext(result -> assertThat(result).isEqualTo("input-done"))
                .verifyComplete();
    }

    @Test
    void stepTimeout_triggersCompensationOnTimeout() {
        List<String> compensated = new ArrayList<>();

        // first completes fast, second is slow and has a timeout
        Saga<String, String> saga = Saga.builder("step-comp", String.class, String.class)
                .step(fastStep("first", compensated))
                .step(slowStep("slow", Duration.ofSeconds(10), compensated))
                .timeout(Duration.ofMillis(50))
                .build();

        StepVerifier.withVirtualTime(() -> saga.execute("input"))
                .thenAwait(Duration.ofMillis(100))
                .expectError(TimeoutException.class)
                .verify();

        // "first" completed and registered local state → must be compensated
        assertThat(compensated).contains("input");
    }

    @Test
    void stepTimeout_onlyAffectsConfiguredStep() {
        // timeout only on "first"; "second" has no timeout
        Saga<String, String> saga = Saga.builder("selective-timeout", String.class, String.class)
                .step(fastStep("first", new ArrayList<>()))
                .timeout(Duration.ofMillis(50))   // applies only to "first"
                .step(fastStep("second", new ArrayList<>()))
                .build();

        StepVerifier.create(saga.execute("input"))
                .assertNext(result -> assertThat(result).isEqualTo("input-done-done"))
                .verifyComplete();
    }

    // ── saga-level timeout tests ──────────────────────────────────────────────

    @Test
    void sagaTimeout_slowExecution_timesOut() {
        Saga<String, String> saga = Saga.builder("saga-timeout", String.class, String.class)
                .timeout(Duration.ofMillis(50))
                .step(slowStep("slow", Duration.ofSeconds(10), new ArrayList<>()))
                .build();

        StepVerifier.withVirtualTime(() -> saga.execute("input"))
                .thenAwait(Duration.ofMillis(100))
                .expectError(TimeoutException.class)
                .verify();
    }

    @Test
    void sagaTimeout_fastExecution_completesBeforeTimeout() {
        Saga<String, String> saga = Saga.builder("saga-fast", String.class, String.class)
                .timeout(Duration.ofSeconds(10))
                .step(fastStep("fast", new ArrayList<>()))
                .build();

        StepVerifier.create(saga.execute("input"))
                .assertNext(result -> assertThat(result).isEqualTo("input-done"))
                .verifyComplete();
    }

    @Test
    void sagaTimeout_coversMultipleSteps() {
        // Each step takes 30ms; saga timeout at 50ms — second step pushes total over limit
        Saga<String, String> saga = Saga.builder("saga-multi-timeout", String.class, String.class)
                .timeout(Duration.ofMillis(50))
                .step(slowStep("first", Duration.ofMillis(30), new ArrayList<>()))
                .step(slowStep("second", Duration.ofMillis(30), new ArrayList<>()))
                .build();

        StepVerifier.withVirtualTime(() -> saga.execute("input"))
                .thenAwait(Duration.ofMillis(100))
                .expectError(TimeoutException.class)
                .verify();
    }
}

