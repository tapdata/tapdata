package com.tapdata.tm.userLog.constant;

/**
 * Audit event categories exposed to administrators.
 */
public enum AuditEventType {
    LOGIN("login"),
    ADMIN_OPERATION("adminOperation"),
    CONFIGURATION_CHANGE("configurationChange"),
    SERVICE_LIFECYCLE("serviceLifecycle"),
    USER_OPERATION("userOperation");

    private final String value;

    AuditEventType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
