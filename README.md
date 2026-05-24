# saga

A reactive, type-safe Saga orchestration library for Java built on [Project Reactor](https://projectreactor.io/).

## Overview

The saga pattern coordinates multi-step distributed transactions with automatic compensation (rollback) when any step fails.
This library provides a fluent, compile-time-safe builder API, a lifecycle observer system, optional resource locking, and pluggable persistence for crash recovery.

## Modules

| Module | Description |
|---|---|
| `core` | Core API and execution engine. No framework dependencies. |
| `engines:in-memory` | Default stateless engine — execution state lives in the reactive chain. |
| `engines:persistent` | Persistent engine — checkpoints step outputs after each step for crash recovery. |
| `spring-starter` | Spring Boot auto-configuration for all of the above. |

## Quick Start

```java
Saga<Order, Receipt> saga = Saga.builder("OrderFlow", Order.class, Receipt.class)
        .step(new ValidateOrderStep())
        .step(new ReserveInventoryStep())
        .step(new ChargePaymentStep())
        .build();

// Execute
Mono<Receipt> result = saga.execute(order);
```

## Defining a Step

Implement `SagaStep<Input, Output, LocalState>`:

```java
public class ReserveInventoryStep implements SagaStep<Order, Reservation, String> {

    @Override public String name() { return "ReserveInventory"; }
    @Override public Class<Order> inputType() { return Order.class; }
    @Override public Class<Reservation> outputType() { return Reservation.class; }

    @Override
    public Mono<StepResult<Reservation, String>> execute(Order order) {
        return inventoryService.reserve(order.itemId(), order.quantity())
                .map(reservationId -> StepResult.of(new Reservation(reservationId), reservationId));
    }

    @Override
    public Mono<Void> compensate(String reservationId) {
        return inventoryService.cancel(reservationId);
    }
}
```

### Input Resolution

Each step receives the output of the previous step directly.
When a step requires fields from **multiple previous steps**, declare its input as a **record** or **interface**
and the engine resolves the fields automatically by name and type from the execution history:

```java
// Downstream step receives both fields resolved from history
record PaymentInput(String reservationId, long totalCents) {}

public class ChargePaymentStep implements SagaStep<PaymentInput, Receipt, String> { … }
```

## Builder API

```java
Saga.builder("MyFlow", Input.class, Output.class)
    .withObserver(observer)                     // lifecycle observer
    .withLock(input -> input.lockableResources()) // exclusive resource locking
    .withCorrelationId(input -> input.id())     // for persistence/resume support
    .withPersistenceHook(hook)                  // crash-recovery hook
    .step(new StepA())
    .step(new StepB())
    .parallel("group", new StepC(), new StepD()) // steps C & D run in parallel
    .injectProperties(configObject)             // inject a config object into the history
    .build();
```

## Parallel Steps

```java
Saga.builder("ParallelFlow", Input.class, Output.class)
        .parallel("fetch-group",
                new FetchUserStep(),    // Void → UserInfo
                new FetchOrderStep())  // Void → OrderInfo
        .step(new MergeStep())         // receives UserInfo + OrderInfo via record mapping
        .build();
```

## Lifecycle Observer

```java
CapturingSagaLifecycleObserver observer = CapturingSagaLifecycleObserver.create();

saga.execute(input, observer).block();

assertThat(observer.getCompletedSagas()).containsExactly("MyFlow");
assertThat(observer.getStartedSteps()).hasSize(3);
assertThat(observer.hasErrors()).isFalse();
```

Custom observers implement `SagaLifecycleObserver` (all methods have default no-op implementations):

```java
public interface SagaLifecycleObserver {
    default void onSagaStarted(String sagaName) {}
    default void onStepStarted(String sagaName, String stepName) {}
    default void onStepCompleted(String sagaName, String stepName) {}
    default void onStepFailed(String sagaName, String stepName, Throwable error) {}
    default void onError(String sagaName, Throwable error) {}
    default void onSagaCompleted(String sagaName) {}
    default void onCompensationCompleted(String sagaName) {}
    default void onCompensationFailed(String sagaName, SagaCompensationException error) {}
}
```

Combine multiple observers with `SagaLifecycleObserver.combine(o1, o2)`.

## Engines

### In-Memory (default)

```java
SagaEngine engine = InMemorySagaEngine.create();
Saga<Order, Receipt> saga = engine.build(
        Saga.builder("OrderFlow", Order.class, Receipt.class)
                .step(new ValidateOrderStep()));
```

### Persistent (crash-recovery)

Requires a `SagaExecutionRepository` implementation for your datastore and a `SagaStateSerializer`:

```java
SagaEngine engine = new PersistentSagaEngine(repository,
        SagaStateSerializer.jacksonWithFqcn(objectMapper));

Saga<Order, Receipt> saga = engine.build(
        Saga.builder("OrderFlow", Order.class, Receipt.class)
                .withCorrelationId(Order::id)      // required for resume support
                .step(new ValidateOrderStep())
                .step(new ReserveInventoryStep()));
```

On restart, if a saga with the same `correlationId` is found `IN_PROGRESS`, completed steps are skipped and execution resumes from the first incomplete step.

Implement `SagaExecutionRepository` for your datastore (R2DBC, MongoDB, Redis, etc.):

```java
@Bean
public SagaExecutionRepository sagaExecutionRepository(R2dbcEntityTemplate template) {
    return new R2dbcSagaExecutionRepository(template);
}
```

## Resource Locking

Prevent concurrent executions for the same resource:

```java
Saga<Order, Receipt> saga = Saga.builder("OrderFlow", Order.class, Receipt.class)
        .withLock(order -> List.of(order.customerId()))
        .step(…)
        .build(engine);
```

Provide a `SagaLockRepository` implementation for your datastore; `SagaLockService` is created automatically.

## Spring Boot Auto-Configuration

Add `spring-starter` to your dependencies. The following beans are registered automatically:

| Bean | Condition |
|---|---|
| `SagaEngine` (in-memory) | Always (unless `SagaEngine` bean already exists) |
| `SagaEngine` (persistent) | When `SagaExecutionRepository` bean is present |
| `SagaLifecycleObserver` (logging) | Unless disabled via `saga.observer.logging-enabled=false` |
| `SagaLockService` | When `SagaLockRepository` bean is present |

### Configuration Properties

```yaml
saga:
  observer:
    logging-enabled: true   # set to false to disable the built-in logging observer
```

## Compensation

When a step fails, all previously completed steps are compensated in **reverse order**. Each step's `compensate(localState)` method is called with the local state it produced during `execute`.

If any compensation steps themselves fail, a `SagaCompensationException` is thrown. It contains:
- `getOriginalError()` — the error that triggered compensation
- `getCompensationFailures()` — per-step compensation failures
