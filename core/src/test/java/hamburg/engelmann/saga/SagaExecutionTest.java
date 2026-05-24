package hamburg.engelmann.saga;

import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SagaExecutionTest {

    // ── helpers ──────────────────────────────────────────────────────────────

    record Greeted(String message) {}
    record UpperCased(String text) {}

    /** Simple step: String → Greeted */
    static SagaStep<String, Greeted, String> greetStep(List<String> compensated) {
        return new SagaStep<>() {
            @Override public String name() { return "greet"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<Greeted> outputType() { return Greeted.class; }
            @Override public Mono<StepResult<Greeted, String>> execute(String input) {
                return Mono.just(StepResult.of(new Greeted("Hello, " + input + "!"), input));
            }
            @Override public Mono<Void> compensate(String state) {
                compensated.add(state);
                return Mono.empty();
            }
        };
    }

    /** Step: Greeted → UpperCased */
    static SagaStep<Greeted, UpperCased, Void> upperCaseStep() {
        return new SagaStep<>() {
            @Override public String name() { return "upper"; }
            @Override public Class<Greeted> inputType() { return Greeted.class; }
            @Override public Class<UpperCased> outputType() { return UpperCased.class; }
            @Override public Mono<StepResult<UpperCased, Void>> execute(Greeted input) {
                return Mono.just(StepResult.stateless(new UpperCased(input.message().toUpperCase())));
            }
        };
    }

    /** Step that always fails */
    static SagaStep<Greeted, UpperCased, Void> failingStep() {
        return new SagaStep<>() {
            @Override public String name() { return "fail"; }
            @Override public Class<Greeted> inputType() { return Greeted.class; }
            @Override public Class<UpperCased> outputType() { return UpperCased.class; }
            @Override public Mono<StepResult<UpperCased, Void>> execute(Greeted input) {
                return Mono.error(new RuntimeException("step failed"));
            }
        };
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void singleStep_happyPath() {
        Saga<String, Greeted> saga = Saga.builder("greet-saga", String.class, Greeted.class)
                .step(greetStep(new ArrayList<>()))
                .build();

        StepVerifier.create(saga.execute("World"))
                .assertNext(result -> assertThat(result.message()).isEqualTo("Hello, World!"))
                .verifyComplete();
    }

    @Test
    void multiStep_happyPath() {
        Saga<String, UpperCased> saga = Saga.builder("multi-saga", String.class, UpperCased.class)
                .step(greetStep(new ArrayList<>()))
                .step(upperCaseStep())
                .build();

        StepVerifier.create(saga.execute("World"))
                .assertNext(result -> assertThat(result.text()).isEqualTo("HELLO, WORLD!"))
                .verifyComplete();
    }

    @Test
    void stepFailure_propagatesError() {
        Saga<String, UpperCased> saga = Saga.builder("fail-saga", String.class, UpperCased.class)
                .step(greetStep(new ArrayList<>()))
                .step(failingStep())
                .build();

        StepVerifier.create(saga.execute("World"))
                .expectErrorMessage("step failed")
                .verify();
    }

    @Test
    void stepFailure_triggersCompensation() {
        List<String> compensated = new ArrayList<>();

        Saga<String, UpperCased> saga = Saga.builder("comp-saga", String.class, UpperCased.class)
                .step(greetStep(compensated))
                .step(failingStep())
                .build();

        StepVerifier.create(saga.execute("World"))
                .expectError()
                .verify();

        assertThat(compensated).containsExactly("World");
    }

    @Test
    void voidInputStep_works() {
        SagaStep<Void, Greeted, Void> voidStep = new SagaStep<>() {
            @Override public String name() { return "void-greet"; }
            @Override public Class<Void> inputType() { return Void.class; }
            @Override public Class<Greeted> outputType() { return Greeted.class; }
            @Override public Mono<StepResult<Greeted, Void>> execute(Void input) {
                return Mono.just(StepResult.stateless(new Greeted("Hello!")));
            }
        };

        Saga<Void, Greeted> saga = Saga.builder("void-saga", Void.class, Greeted.class)
                .step(voidStep)
                .build();

        StepVerifier.create(saga.execute(null))
                .assertNext(result -> assertThat(result.message()).isEqualTo("Hello!"))
                .verifyComplete();
    }

    @Test
    void injectProperties_makesPropertiesAvailable() {
        record Config(String prefix) {}
        Config config = new Config("Hi");

        SagaStep<Config, Greeted, Void> configStep = new SagaStep<>() {
            @Override public String name() { return "config-greet"; }
            @Override public Class<Config> inputType() { return Config.class; }
            @Override public Class<Greeted> outputType() { return Greeted.class; }
            @Override public Mono<StepResult<Greeted, Void>> execute(Config input) {
                return Mono.just(StepResult.stateless(new Greeted(input.prefix() + " World")));
            }
        };

        Saga<Void, Greeted> saga = Saga.builder("inject-saga", Void.class, Greeted.class)
                .injectProperties(config)
                .step(configStep)
                .build();

        StepVerifier.create(saga.execute(null))
                .assertNext(r -> assertThat(r.message()).isEqualTo("Hi World"))
                .verifyComplete();
    }

    @Test
    void saga_hasName_and_outputClass() {
        Saga<String, Greeted> saga = Saga.builder("named-saga", String.class, Greeted.class)
                .step(greetStep(new ArrayList<>()))
                .build();

        assertThat(saga.getName()).isEqualTo("named-saga");
        assertThat(saga.getOutputClass()).isEqualTo(Greeted.class);
    }

    @Test
    void emptyStepOutput_doesNotBreakChain() {
        SagaStep<String, StepResult.EmptyOutput, Void> emptyStep = new SagaStep<>() {
            @Override public String name() { return "noop"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Class<StepResult.EmptyOutput> outputType() { return StepResult.EmptyOutput.class; }
            @Override public Mono<StepResult<StepResult.EmptyOutput, Void>> execute(String input) {
                return Mono.just(StepResult.empty());
            }
        };

        Saga<String, StepResult.EmptyOutput> saga = Saga.builder("empty-saga", String.class, StepResult.EmptyOutput.class)
                .step(emptyStep)
                .build();

        StepVerifier.create(saga.execute("x"))
                .assertNext(r -> assertThat(r).isNotNull())
                .verifyComplete();
    }

    @Test
    void recordInputMapping_resolvesFromHistory() {
        record UserId(String id) {}
        record UserName(String name) {}
        record UserProfile(String id, String name) {}

        SagaStep<Long, UserId, Void> idStep = new SagaStep<>() {
            @Override public String name() { return "id"; }
            @Override public Class<Long> inputType() { return Long.class; }
            @Override public Class<UserId> outputType() { return UserId.class; }
            @Override public Mono<StepResult<UserId, Void>> execute(Long input) {
                return Mono.just(StepResult.stateless(new UserId("u-" + input)));
            }
        };
        SagaStep<UserId, UserName, Void> nameStep = new SagaStep<>() {
            @Override public String name() { return "name"; }
            @Override public Class<UserId> inputType() { return UserId.class; }
            @Override public Class<UserName> outputType() { return UserName.class; }
            @Override public Mono<StepResult<UserName, Void>> execute(UserId input) {
                return Mono.just(StepResult.stateless(new UserName("User " + input.id())));
            }
        };
        SagaStep<UserProfile, UserProfile, Void> profileStep = new SagaStep<>() {
            @Override public String name() { return "profile"; }
            @Override public Class<UserProfile> inputType() { return UserProfile.class; }
            @Override public Class<UserProfile> outputType() { return UserProfile.class; }
            @Override public Mono<StepResult<UserProfile, Void>> execute(UserProfile input) {
                return Mono.just(StepResult.stateless(input));
            }
        };

        Saga<Long, UserProfile> saga = Saga.builder("profile-saga", Long.class, UserProfile.class)
                .step(idStep)
                .step(nameStep)
                .step(profileStep)
                .build();

        StepVerifier.create(saga.execute(1L))
                .assertNext(p -> {
                    assertThat(p.id()).isEqualTo("u-1");
                    assertThat(p.name()).isEqualTo("User u-1");
                })
                .verifyComplete();
    }
}
