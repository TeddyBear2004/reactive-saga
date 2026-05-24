package hamburg.engelmann.saga.lock;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

/** Outbound port — implemented by the infrastructure layer (e.g. R2DBC repository). */
public interface SagaLockRepository {

    Mono<SagaLock> save(SagaLock lock);

    Flux<SagaLock> findBySagaId(UUID sagaId);

}
