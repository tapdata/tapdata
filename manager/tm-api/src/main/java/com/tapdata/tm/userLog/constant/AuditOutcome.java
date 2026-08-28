package com.tapdata.tm.userLog.constant;

public enum AuditOutcome {
    SUCCESS("success"),
    FAILURE("failure");

    private final String value;

    AuditOutcome(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
