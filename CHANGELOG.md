# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [1.3.0] – 2026-05-25

### Added

#### Step-level locking via `SagaStepContext`

Steps can now acquire locks **mid-execution** — for resources whose IDs are only known
after async work inside `execute()` completes.

```java
@Override
public Mono<StepResult<Output, State>> execute(Input input, SagaStepContext ctx) {
    return repository.create(input)
        .flatMap(entity -> ctx.acquireLock(new EntityLock(entity.getId()))
            .thenReturn(entity))
        .flatMap(this::process);
}
```

Locks acquired via `SagaStepContext` are handed to the saga and held until saga end:
- **Success / cancellation** → `RELEASED`
- **Error / compensation** → `FAILED`

The new `execute(I input, SagaStepContext ctx)` overload is optional and backward-compatible.
Steps that only implement `execute(I input)` continue to work without changes.

#### Deferred locks via `withDeferredLock()`

For resources whose ID is produced by a step's *output* (not computed inside the step),
use `withDeferredLock()` on the step builder:

```java
Saga.builder("OrderFlow", Input.class, Output.class)
    .step(new SaveOrderStep())                       // produces OrderId in its output
    .withDeferredLock(OrderLock.class)               // lock acquired after SaveOrderStep, using OrderId
    .step(new ProcessOrderStep())
    .build();
```

`OrderLock` is a `LockSpec` record — the framework resolves its fields from the saga
execution history at the point where the lock is registered. The lock is held for the
remainder of the saga.

#### New `SagaLockService` lifecycle methods

| Method | Description |
|--------|-------------|
| `acquireLocks(context, resources)` | Acquires locks and returns the `SagaLock` records |
| `releaseAcquiredLocks(locks)` | Marks locks as `RELEASED` |
| `failAcquiredLocks(locks)` | Marks locks as `FAILED` |

These methods are used internally by the new locking mechanisms. They are also available
for custom integrations that need fine-grained lock lifecycle control.

### Fixed

- **Retry re-invocation**: Steps decorated with `.retryable()` now correctly re-invoke
  `execute()` on each retry attempt. Previously, the Mono returned by `execute()` was
  re-subscribed without calling `execute()` again, which broke stateful step implementations
  (e.g. attempt counters, non-deferred Monos). The fix wraps step execution in `Mono.defer()`
  so each retry triggers a fresh call to `execute()`.

---

## [1.2.0] – 2026-05-24

### Added

#### DI-based input resolution

Steps can now declare multi-field input records, and the engine resolves field values
automatically from the saga execution history by name and type:

```java
record PaymentInput(String reservationId, long totalCents) {}

public class ChargePaymentStep implements SagaStep<PaymentInput, Receipt, String> { … }
```

No explicit mapping code is needed. The engine matches `reservationId` and `totalCents`
against the outputs of all previously executed steps.

#### Proxy injection via `injectProperties()`

Inject arbitrary configuration or context objects into the execution history so that
downstream steps can receive them through the same DI mechanism:

```java
Saga.builder("OrderFlow", Input.class, Output.class)
    .injectProperties(pricingConfig)
    .step(new ValidateStep())
    .step(new PriceStep())   // receives PricingConfig via record DI
    .build();
```

#### `withLock(Class<?>)` builder overload

Lock specs can now be declared by class and resolved from the saga input via DI,
complementing the existing `withLock(Function<I, LockSpec>)` form:

```java
Saga.builder("OrderFlow", OrderInput.class, Output.class)
    .withLock(OrderLock.class)   // resolved from OrderInput fields by DI
    .withSagaLockService(lockService)
    .step(…)
    .build();
```

---

## [1.1.0] – 2026-05-23

### Added

- **Retry support**: `.retryable(int maxAttempts)` and `.retryable(int maxAttempts, Duration fixedDelay)` on `SagaStepBuilder`.
- **Timeout support**: `.timeout(Duration)` on `SagaStepBuilder`.

---

## [1.0.2] – 2026-05-22

### Changed

- Migrated package namespace to `hamburg.engelmann`.

---

## [1.0.0] – 2026-05-22

Initial release.

### Included

- Fluent `Saga.builder()` API with compile-time type safety.
- Sequential and parallel step execution.
- Automatic compensation (reverse-order rollback on failure).
- `SagaLifecycleObserver` interface with default no-op implementations.
- `LoggingSagaLifecycleObserver` and `CapturingSagaLifecycleObserver`.
- `SagaLockService` / `SagaLockRepository` for exclusive resource locking.
- `InMemorySagaEngine` and `PersistentSagaEngine` (Jackson FQCN serialization, crash-recovery resume).
- Spring Boot auto-configuration via `spring-starter`.
