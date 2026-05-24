package hamburg.engelmann.saga.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class SagaExecutionContext {

    private final List<Object> executionOutputs = new ArrayList<>();
    private final List<RollbackRecord> rollbackStack = new ArrayList<>();
    private final int completedTransitions;

    SagaExecutionContext(Object initialPayload) {
        this.completedTransitions = 0;
        this.executionOutputs.add(initialPayload);
    }

    /**
     * Resume constructor: pre-populates outputs from a previous execution.
     * {@code completedTransitions} tracks how many transitions to skip in the chain.
     * {@code preloadedOutputs} is the flat output list (excluding the initial payload which
     * is added separately as the first entry).
     */
    SagaExecutionContext(Object initialPayload, List<Object> preloadedOutputs, int completedTransitions) {
        this.completedTransitions = completedTransitions;
        this.executionOutputs.add(initialPayload);
        this.executionOutputs.addAll(preloadedOutputs);
    }

    /** How many transitions were already completed before this execution began (resume offset). */
    int getCompletedTransitions() { return completedTransitions; }

    List<Object> getExecutionOutputs() { return executionOutputs; }

    void addOutput(Object output) {
        if (output instanceof ParallelOutputs(List<Object> values)) {
            executionOutputs.addAll(values);
        } else {
            executionOutputs.add(output);
        }
    }

    void addNullOutput() { executionOutputs.add(null); }

    void addRollbackRecord(SagaTransition<?, ?, ?> transition, Object localState) {
        rollbackStack.add(new RollbackRecord(transition, localState));
    }

    int getRollbackStackSize() { return rollbackStack.size(); }

    List<RollbackRecord> getReversedRollbackStack() {
        List<RollbackRecord> reversed = new ArrayList<>(rollbackStack);
        Collections.reverse(reversed);
        return reversed;
    }

}
