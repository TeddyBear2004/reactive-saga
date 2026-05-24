package com.saga.lock;

import java.util.Objects;
import java.util.UUID;

public record SagaLockId(UUID value) {

    public SagaLockId {
        Objects.requireNonNull(value, "SagaLockId value cannot be null");
    }

    @Override
    public String toString() { return value.toString(); }

}
