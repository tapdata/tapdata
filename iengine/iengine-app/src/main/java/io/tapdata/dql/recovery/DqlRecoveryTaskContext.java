package io.tapdata.dql.recovery;

/** Task facts read by Engine before accepting a recovery command. */
public record DqlRecoveryTaskContext(String taskId, Long taskVersion, String agentId) {
}
