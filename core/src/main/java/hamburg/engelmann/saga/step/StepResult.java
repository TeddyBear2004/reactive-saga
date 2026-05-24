package hamburg.engelmann.saga.step;

/**
 * Container for the result produced by one saga step.
 *
 * @param output     value forwarded to the next step
 * @param localState compensation-relevant state stored per step
 */
public record StepResult<O, L>(O output, L localState) {

    public static <O, L> StepResult<O, L> of(O output, L localState) {
        return new StepResult<>(output, localState);
    }

    public static <O> StepResult<O, Void> stateless(O output) {
        return new StepResult<>(output, null);
    }

    public static <L> StepResult<EmptyOutput, L> noOutput(L localState) {
        return new StepResult<>(EMPTY_OUTPUT, localState);
    }

    public static StepResult<EmptyOutput, Void> empty() {
        return new StepResult<>(EMPTY_OUTPUT, null);
    }

    /** Singleton sentinel used when a step produces no meaningful output. */
    public static final EmptyOutput EMPTY_OUTPUT = new EmptyOutput();

    /**
     * Sentinel type for steps that produce no meaningful output.
     *
     * <p>Only {@link StepResult} can instantiate this class. Use the
     * factory methods {@link #empty()} or {@link #noOutput(Object)} rather than
     * constructing directly.
     */
    public static final class EmptyOutput {

        private EmptyOutput() {}

        @Override
        public String toString() { return "EmptyOutput"; }

    }

}

