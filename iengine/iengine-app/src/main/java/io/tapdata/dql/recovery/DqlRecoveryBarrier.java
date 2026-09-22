package io.tapdata.dql.recovery;

/** Waits until the current recovery event has a terminal task outcome. */
@FunctionalInterface
public interface DqlRecoveryBarrier {
    /** Registers failure delivery before the DML enters the processing graph. */
    default void register(String eventId) {
    }

    /** Releases a registration when event injection fails before await. */
    default void cancel(String eventId) {
    }

    Outcome await(String eventId, long timeoutMillis) throws InterruptedException;

    /**
     * Waits for the event and retains the failure details delivered by the
     * target capture boundary.  The default keeps older barrier
     * implementations source-compatible while allowing the production
     * coordinator to return the original connector exception.
     */
    default Result awaitResult(String eventId, long timeoutMillis) throws InterruptedException {
        return new Result(await(eventId, timeoutMillis), null, null);
    }

    record Result(Outcome outcome, String message, String errorDetails) {
    }

    enum Outcome {
        SUCCESS,
        FAILED,
        TIMEOUT
    }
}
