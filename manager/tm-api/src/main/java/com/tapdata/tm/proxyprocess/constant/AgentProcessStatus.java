package com.tapdata.tm.proxyprocess.constant;

import org.apache.commons.lang3.StringUtils;

public final class AgentProcessStatus {
    public static final String RUNNING = "RUNNING";
    public static final String OFFLINE = "OFFLINE";
    public static final String FAILED = "FAILED";

    private AgentProcessStatus() {
    }

    public static boolean valid(String status) {
        if (StringUtils.isBlank(status)) {
            return false;
        }
        status = status.toUpperCase();
        return RUNNING.equals(status)
                || OFFLINE.equals(status)
                || FAILED.equals(status);
    }
}
