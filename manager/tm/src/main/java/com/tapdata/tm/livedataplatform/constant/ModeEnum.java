package com.tapdata.tm.livedataplatform.constant;

public enum ModeEnum {
    INTEGRATION_PLATFORM("integration"),
    SERVICE_PLATFORM("service")
    ;

    private final String value;

    // 构造方法
    private ModeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}

