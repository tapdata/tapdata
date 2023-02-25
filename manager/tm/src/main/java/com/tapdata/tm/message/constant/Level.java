package com.tapdata.tm.message.constant;

public enum Level {
    RECOVERY("Recovery"),
    NORMAL("Normal"),
    WARNING("Warning"),
    CRITICAL("Critical"),
    EMERGENCY("Emergency"),

    ERROR("Error"),

    //校验内容有差异
    WARN("Warn"),

    INFO("Info");

    private final String value;
    public String getValue() {
        return value;
    }
    private Level(String value) {
        this.value = value;
    }
}
