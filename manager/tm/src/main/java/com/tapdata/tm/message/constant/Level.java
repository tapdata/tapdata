package com.tapdata.tm.message.constant;

public enum Level {
    RECOVERY("RECOVERY"),
    NORMAL("NORMAL"),
    WARNING("WARNING"),
    CRITICAL("CRITICAL"),
    EMERGENCY("EMERGENCY"),

    ERROR("ERROR"),

    //校验内容有差异
    WARN("WARN"),

    INFO("INFO");

    private final String value;
    public String getValue() {
        return value;
    }
    private Level(String value) {
        this.value = value;
    }
}
