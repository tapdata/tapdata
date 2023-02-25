package com.tapdata.tm.Settings.constant;

public enum KeyEnum {
    NOTIFICATION("notification"),
    JOB_HEART_TIMEOUT("jobHeartTimeout"),
    WORKER_HEART_TIMEOUT("lastHeartbeat"),

    EMAIL_HREF("emailHref"),
    ALLOW_CONNECTION_TYPE("ALLOW_CONNECTION_TYPE"),
    EMAIL_RECEIVER("email.receivers"),
    EMAIL_TITLE_PREFIX("email.title.prefix"),
    BUILD_PROFILE("buildProfile")
    ;

    private String value;

    KeyEnum(String value) {
        this.value = value;
    }


    public String getValue() {
        return value;
    }
}
