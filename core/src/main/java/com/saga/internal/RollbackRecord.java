package com.saga.internal;

record RollbackRecord(SagaTransition<?, ?, ?> transition, Object localState) { }
