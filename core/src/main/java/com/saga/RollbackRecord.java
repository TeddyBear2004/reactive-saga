package com.saga;

record RollbackRecord(SagaTransition<?, ?, ?> transition, Object localState) { }
