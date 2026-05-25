# saga

A reactive, type-safe Saga orchestration library for Java built on [Project Reactor](https://projectreactor.io/).

## Overview

Use this library to coordinate multi-step distributed transactions with automatic compensation (rollback) when any step fails.
You get a fluent, compile-time-safe builder API and a lifecycle observer system. You can also add optional resource locking and pluggable persistence for crash recovery.

## Modules

| Module | Description |
|---|---|
| `core` | Core API and execution engine. No framework dependencies. |
| `engines:in-memory` | Default stateless engine — execution state lives in the reactive chain. |
| `engines:persistent` | Persistent engine — checkpoints step outputs after each step for crash recovery. |
| `spring-starter` | Spring Boot auto-configuration for all of the above. |

## Quick Start

### 1. Add the dependency

```xml
<!-- Maven -->
<dependency>
  <groupId>hamburg.engelmann</groupId>
  <artifactId>saga-core</artifactId>
  <version>1.3.0</version>
</dependency>
```

```kotlin
// Gradle (Kotlin DSL)
implementation("hamburg.engelmann:saga-core:1.3.0")
```

### 2. Build and execute a saga

```java
import hamburg.engelmann.saga.Saga;
import reactor.core.publisher.Mono;

Saga<Order, Receipt> saga = Saga.builder("OrderFlow", Order.class, Receipt.class)
        .step(new ValidateOrderStep())
        .step(new ReserveInventoryStep())
        .step(new ChargePaymentStep())
        .build();

Mono<Receipt> result = saga.execute(order);
```

## Defining a Step

Implement `SagaStep<Input, Output, LocalState>`:

```java
import hamburg.engelmann.saga.step.SagaStep;
import hamburg.engelmann.saga.step.StepResult;
import reactor.core.publisher.Mono;

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
import hamburg.engelmann.saga.step.SagaStep;

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
import hamburg.engelmann.saga.lifecycle.CapturingSagaLifecycleObserver;

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

Add `engines:in-memory` to your dependencies (`hamburg.engelmann:saga-engine-in-memory`):

```java
import hamburg.engelmann.saga.Saga;
import hamburg.engelmann.saga.SagaEngine;
import hamburg.engelmann.saga.engine.memory.InMemorySagaEngine;

SagaEngine engine = InMemorySagaEngine.create();
Saga<Order, Receipt> saga = engine.build(
        Saga.builder("OrderFlow", Order.class, Receipt.class)
                .step(new ValidateOrderStep()));
```

### Persistent (crash-recovery)

Add `engines:persistent` to your dependencies (`hamburg.engelmann:saga-engine-persistent`).

Requires a `SagaExecutionRepository` implementation for your datastore and a `SagaStateSerializer`:

```java
import hamburg.engelmann.saga.Saga;
import hamburg.engelmann.saga.SagaEngine;
import hamburg.engelmann.saga.engine.persistent.PersistentSagaEngine;
import hamburg.engelmann.saga.engine.persistent.SagaStateSerializer;

SagaEngine engine = new PersistentSagaEngine(repository,
        SagaStateSerializer.jacksonWithFqcn(objectMapper));

Saga<Order, Receipt> saga = engine.build(
        Saga.builder("OrderFlow", Order.class, Receipt.class)
                .withCorrelationId(Order::id)      // required for resume support
                .step(new ValidateOrderStep())
                .step(new ReserveInventoryStep()));
```

On restart, if a saga with the same `correlationId` is found `IN_PROGRESS`, the engine skips completed steps and resumes execution from the first incomplete step.

Implement `SagaExecutionRepository` for your datastore (R2DBC (Reactive Relational Database Connectivity), MongoDB, Redis, etc.):

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
        .withSagaLockService(lockService)
        .step(…)
        .build();
```

Provide a `SagaLockRepository` implementation for your datastore:

```java
SagaLockService lockService = SagaLockService.create(myLockRepository);
```

### Deferred locks (post-step)

When the resource ID is produced as a step's *output*, declare the lock after that step:

```java
Saga.builder("OrderFlow", Input.class, Output.class)
        .step(new SaveOrderStep())         // output contains OrderId
        .withDeferredLock(OrderLock.class) // acquired after SaveOrderStep using OrderId
        .step(new ProcessOrderStep())
        .withSagaLockService(lockService)
        .build();
```

### Mid-step locks (`SagaStepContext`)

When the resource ID is only known *inside* the step (e.g. discovered via an async call),
override the two-argument `execute` overload and call `ctx.acquireLock()`:

```java
public class CreateOrderStep implements SagaStep<Input, Order, Void> {

    @Override
    public Mono<StepResult<Order, Void>> execute(Input input, SagaStepContext ctx) {
        return repository.create(input)
                .flatMap(order -> ctx.acquireLock(new OrderLock(order.getId()))
                        .thenReturn(order))
                .map(StepResult::stateless);
    }
}
```

Locks acquired via `SagaStepContext` are held until the saga ends — released on success,
marked failed on error. Steps that do not override this method continue to work unchanged.

## Spring Boot Auto-Configuration

Add `spring-starter` to your dependencies. Spring Boot registers the following beans automatically:

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

If any compensation steps themselves fail, the engine throws a `SagaCompensationException`. It contains:
- `getOriginalError()` — the error that triggered compensation
- `getCompensationFailures()` — per-step compensation failures

## Next Steps

- Implement `SagaExecutionRepository` to add crash-recovery support for your datastore (R2DBC, MongoDB, Redis).
- Implement `SagaLockRepository` to prevent concurrent executions for the same resource.
- Use `SagaLifecycleObserver.combine(o1, o2)` to stack logging, metrics, and tracing observers.
- Add `spring-starter` to integrate with Spring Boot auto-configuration.
