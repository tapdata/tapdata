package io.tapdata.dql.reporter;

/**
 * Signals that an Engine DLQ event could not be confirmed as persisted by TM.
 */
public class DqlEventReportException extends RuntimeException {
    private final String taskId;

    public DqlEventReportException(String taskId, Throwable cause) {
        super("DLQ event report failed for task " + taskId, cause);
        this.taskId = taskId;
    }

    public DqlEventReportException(String taskId, String reason) {
        super("DLQ event report failed for task " + taskId + ": " + reason);
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }
}
