package io.tapdata.dql.recovery;

import com.tapdata.tm.commons.task.dto.TaskDto;

import java.util.Objects;

/** Immutable task state captured before a DQL replay stops the formal job. */
public record DqlRecoveryTaskSnapshot(TaskDto task,
                                      String statusBefore,
                                      boolean snapshotInterruptBefore) {
    public DqlRecoveryTaskSnapshot(TaskDto task, String statusBefore) {
        this(task, statusBefore, task != null && task.isSnapShotInterrupt());
    }

    public DqlRecoveryTaskSnapshot {
        Objects.requireNonNull(task, "task must not be null");
        if (task.getId() == null) {
            throw new IllegalArgumentException("task id must not be null");
        }
        if (statusBefore == null || statusBefore.isBlank()) {
            throw new IllegalArgumentException("task status must not be blank");
        }
    }

    public boolean wasRunning() {
        return TaskDto.STATUS_RUNNING.equalsIgnoreCase(statusBefore);
    }

    public boolean wasStopped() {
        return TaskDto.STATUS_STOP.equalsIgnoreCase(statusBefore);
    }
}
