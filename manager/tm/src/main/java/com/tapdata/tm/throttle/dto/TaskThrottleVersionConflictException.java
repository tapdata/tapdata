package com.tapdata.tm.throttle.dto;

import com.tapdata.tm.commons.throttle.ThrottleConstants;

/**
 * Thrown on desiredVersion CAS mismatch so the REST controller can respond with HTTP 409 and latestView.
 */
public class TaskThrottleVersionConflictException extends RuntimeException {

    private final String errorCode;
    private final long latestVersion;
    private final TaskThrottlePolicyView latestView;

    public TaskThrottleVersionConflictException(long latestVersion, TaskThrottlePolicyView latestView) {
        super("Task throttle policy version conflict; latestVersion=" + latestVersion);
        this.errorCode = ThrottleConstants.ERR_VERSION_CONFLICT;
        this.latestVersion = latestVersion;
        this.latestView = latestView;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public long getLatestVersion() {
        return latestVersion;
    }

    public TaskThrottlePolicyView getLatestView() {
        return latestView;
    }
}
