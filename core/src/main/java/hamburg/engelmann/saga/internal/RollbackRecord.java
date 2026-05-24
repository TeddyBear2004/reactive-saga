package hamburg.engelmann.saga.internal;

record RollbackRecord(SagaTransition<?, ?, ?> transition, Object localState) { }
