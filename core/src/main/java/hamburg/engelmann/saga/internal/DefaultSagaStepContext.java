package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.lock.Lockable;
import hamburg.engelmann.saga.lock.LockSpec;
import hamburg.engelmann.saga.lock.SagaLock;
import hamburg.engelmann.saga.lock.SagaLockContext;
import hamburg.engelmann.saga.lock.SagaLockService;
import hamburg.engelmann.saga.lock.SagaStepContext;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Default {@link SagaStepContext} implementation.
 * Acquires locks via the saga's {@link SagaLockService} and accumulates them in a
 * shared list so the saga engine can release them all at the end of the execution.
 */
class DefaultSagaStepContext implements SagaStepContext {

    private final SagaLockService lockService;
    private final SagaLockContext lockContext;
    private final List<SagaLock> accumulator;

    DefaultSagaStepContext(SagaLockService lockService, SagaLockContext lockContext,
                            List<SagaLock> accumulator) {
        this.lockService = lockService;
        this.lockContext = lockContext;
        this.accumulator = accumulator;
    }

    @Override
    public Mono<Void> acquireLock(LockSpec lockSpec) {
        List<? extends Lockable> resources = lockSpec.lockableResources();
        return lockService
                .acquireLocks(lockContext, resources.stream().map(Lockable::getId).toList())
                .doOnNext(accumulator::addAll)
                .then();
    }

}
