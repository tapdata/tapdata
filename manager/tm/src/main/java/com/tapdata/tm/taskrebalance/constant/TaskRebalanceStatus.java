package com.tapdata.tm.taskrebalance.constant;

public final class TaskRebalanceStatus {
    private TaskRebalanceStatus() {
    }

    public static final String RUNNING = "RUNNING";
    public static final String OK = "OK";
    public static final String FAILED = "FAILED";
    public static final String CANCELLED = "CANCELLED";

    public static String finalStatus(int failedCount, int cancelledCount) {
        if (failedCount > 0) {
            return FAILED;
        }
        if (cancelledCount > 0) {
            return CANCELLED;
        }
        return OK;
    }
}
