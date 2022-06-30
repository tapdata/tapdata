package com.tapdata.tm.inspect.constant;


public enum InspectResultEnum {
    PASSED("passed"),
    FAILED("failed");

    private final String value;

    // 构造方法
    private InspectResultEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
