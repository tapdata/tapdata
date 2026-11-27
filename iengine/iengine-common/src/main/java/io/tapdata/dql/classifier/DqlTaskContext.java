package io.tapdata.dql.classifier;

import java.util.Locale;

/**
 * Task state and feature snapshot available while classifying an Engine failure.
 */
public final class DqlTaskContext {
    private final String taskType;
    private final String taskStatus;
    private final boolean skipDataEnabled;
    private final boolean dqlEnabled;
    private final boolean retryExhausted;
    private final boolean configurationValid;

    public DqlTaskContext(String taskType,
                          String taskStatus,
                          boolean skipDataEnabled,
                          boolean dqlEnabled,
                          boolean retryExhausted,
                          boolean configurationValid) {
        this.taskType = taskType;
        this.taskStatus = taskStatus;
        this.skipDataEnabled = skipDataEnabled;
        this.dqlEnabled = dqlEnabled;
        this.retryExhausted = retryExhausted;
        this.configurationValid = configurationValid;
    }

    public static DqlTaskContext runningSync() {
        return runningSync(false);
    }

    public static DqlTaskContext runningSync(boolean retryExhausted) {
        return new DqlTaskContext("sync", "RUNNING", true, true, retryExhausted, true);
    }

    public String getTaskType() {
        return taskType;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public boolean isSkipDataEnabled() {
        return skipDataEnabled;
    }

    public boolean isDqlEnabled() {
        return dqlEnabled;
    }

    public boolean isRetryExhausted() {
        return retryExhausted;
    }

    public boolean isConfigurationValid() {
        return configurationValid;
    }

    public boolean isRecordDlqTask() {
        return taskType != null && ("sync".equals(taskType.toLowerCase(Locale.ROOT))
                || "migrate".equals(taskType.toLowerCase(Locale.ROOT)));
    }
}
