package com.tapdata.tm.commons.alarm;

public enum Level {
    RECOVERY("Recovery"),
    NORMAL("Normal"),
    WARNING("Warning"),
    CRITICAL("Critical"),
    EMERGENCY("Emergency"),

    ERROR("Error"),

    //校验内容有差异
    WARN("Warn"),

    INFO("Info"),
    DEBUG("Debug");

    private final String value;
    public String getValue() {
        return value;
    }
    private Level(String value) {
        this.value = value;
    }
}
