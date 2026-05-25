package hamburg.engelmann.saga.lock;

import reactor.core.publisher.Mono;

/**
 * Provided to a {@link hamburg.engelmann.saga.step.SagaStep} during execution,
 * allowing it to acquire locks whose resource IDs are only known at runtime.
 *
 * <p>Locks acquired via this context are held until the end of the saga
 * (success, failure, or compensation), preventing race conditions between
 * the moment the resource is identified and the moment the lock is acquired.
 *
 * <pre>{@code
 * @Override
 * public Mono<StepResult<Output, State>> execute(Input input, SagaStepContext ctx) {
 *     return repo.findById(input.getId())
 *         .flatMap(entity -> ctx.acquireLock(new EntityLock(entity.getId()))
 *             .thenReturn(entity))
 *         .flatMap(entity -> process(entity));
 * }
 * }</pre>
 */
public interface SagaStepContext {

    /**
     * Acquires locks for all resources in the given {@link LockSpec} and hands them
     * over to the saga. The locks are held until the saga completes.
     *
     * @throws ResourceAlreadyLockedException if any resource is already locked
     */
    Mono<Void> acquireLock(LockSpec lockSpec);

    /**
     * Convenience overload: acquires locks for the given individual resources.
     */
    default Mono<Void> acquireLock(Lockable... resources) {
        return acquireLock(() -> java.util.List.of(resources));
    }

}
