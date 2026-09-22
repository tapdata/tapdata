package io.tapdata.dql.recovery;

/** Reads the current task assignment and version from Engine's TM view. */
@FunctionalInterface
public interface DqlRecoveryTaskContextProvider {
    DqlRecoveryTaskContext find(String taskId);
}
