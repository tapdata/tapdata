package io.tapdata.dql.recovery;

/** Decides whether a batch should move to the next event after a failure. */
@FunctionalInterface
public interface DqlRecoveryExecutionPolicy {
    boolean continueAfterFailure();
}
