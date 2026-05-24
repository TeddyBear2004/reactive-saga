package com.saga.lock;

import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode(of = "id")
@ToString
public class SagaLock {

    private SagaLockId id;
    private UUID sagaId;
    private String sagaName;
    private String resourceType;
    private String resourceId;
    private String userId;
    private Instant startedAt;
    private Instant releasedAt;
    private SagaLockStatus status;

}
