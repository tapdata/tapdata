package io.tapdata.flow.engine.V2.throttle;

import java.io.Serializable;
import java.util.Objects;

/**
 * Immutable identity key for an authorized task execution epoch inside an Agent.
 */
public final class TaskRunKey implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String tenantId;
    private final String taskKind;
    private final String taskId;
    private final long runEpoch;

    public TaskRunKey(String tenantId, String taskKind, String taskId, long runEpoch) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (runEpoch < 0L) {
            throw new IllegalArgumentException("runEpoch must be non-negative");
        }
        this.tenantId = tenantId == null ? "" : tenantId.trim();
        this.taskKind = (taskKind == null || taskKind.isBlank()) ? "sync" : taskKind.trim();
        this.taskId = taskId.trim();
        this.runEpoch = runEpoch;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getTaskKind() {
        return taskKind;
    }

    public String getTaskId() {
        return taskId;
    }

    public long getRunEpoch() {
        return runEpoch;
    }

    /**
     * Canonical TM task key formatted as {@code tenantId:taskKind:taskId}.
     */
    public String toTaskKey() {
        return tenantId + ":" + taskKind + ":" + taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TaskRunKey other)) {
            return false;
        }
        return runEpoch == other.runEpoch
                && Objects.equals(tenantId, other.tenantId)
                && Objects.equals(taskKind, other.taskKind)
                && Objects.equals(taskId, other.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, taskKind, taskId, runEpoch);
    }

    @Override
    public String toString() {
        return toTaskKey() + "@" + runEpoch;
    }
}
