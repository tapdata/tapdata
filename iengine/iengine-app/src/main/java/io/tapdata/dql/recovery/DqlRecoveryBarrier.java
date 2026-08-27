package io.tapdata.dql.recovery;

/** Waits until the current recovery event has a terminal task outcome. */
@FunctionalInterface
public interface DqlRecoveryBarrier {
    Outcome await(String eventId, long timeoutMillis) throws InterruptedException;

    enum Outcome {
        SUCCESS,
        FAILED,
        TIMEOUT
    }
}
