package com.tapdata.tm.modules.constant;

public enum ModuleStatusEnum {
    ACTIVE("active"),
    PENDING("pending") ;

    private final String value;

    // 构造方法
    private ModuleStatusEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
