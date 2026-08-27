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

    enum Outcome {
        SUCCESS,
        FAILED,
        TIMEOUT
    }
}
