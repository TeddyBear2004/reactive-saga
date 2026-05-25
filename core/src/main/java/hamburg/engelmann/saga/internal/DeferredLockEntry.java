package hamburg.engelmann.saga.internal;

import hamburg.engelmann.saga.di.internal.InputResolver;
import hamburg.engelmann.saga.lock.LockSpec;

/**
 * A deferred resource lock acquired after a specific saga step completes,
 * rather than at saga start. Used when resource IDs are only known after a step runs.
 *
 * @param afterTransitionIndex zero-based index of the transition after which the lock fires
 * @param resolver             resolves the {@link LockSpec} from the saga history at lock time,
 *                             including the triggering step's output
 */
record DeferredLockEntry(int afterTransitionIndex, InputResolver<? extends LockSpec> resolver) {}
