package com.tapdata.tm.taskrebalance.constant;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class TaskRebalanceJobStatus {
    private TaskRebalanceJobStatus() {
    }

    public static final String PENDING = "PENDING";
    public static final String INVALID_AGENT = "INVALID_AGENT";
    public static final String STATUS_ERROR = "STATUS_ERROR";
    public static final String STOPPING = "STOPPING";
    public static final String STOP_TIMEOUT = "STOP_TIMEOUT";
    public static final String STARTING = "STARTING";
    public static final String START_TIMEOUT = "START_TIMEOUT";
    public static final String OK = "OK";
    public static final String CANCELLED = "CANCELLED";

    public static final Set<String> TERMINAL_STATUS = new HashSet<>(Arrays.asList(
            INVALID_AGENT,
            STATUS_ERROR,
            STOP_TIMEOUT,
            START_TIMEOUT,
            OK,
            CANCELLED
    ));

    public static final Set<String> ACTIVE_STATUS = new HashSet<>(Arrays.asList(
            PENDING,
            STOPPING,
            STARTING
    ));

    public static boolean isTerminal(String status) {
        return TERMINAL_STATUS.contains(status);
    }
}
