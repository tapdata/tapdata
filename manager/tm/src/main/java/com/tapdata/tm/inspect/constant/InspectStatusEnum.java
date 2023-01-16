package com.tapdata.tm.inspect.constant;

public enum InspectStatusEnum {
    PASSED("passed"),
    ERROR("error"),
    RUNNING("running"),
    FAILED("failed"),
    DONE("done"),
    WAITING("waiting"),
    SCHEDULING("scheduling")  ;

    private final String value;

    // 构造方法
    private InspectStatusEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
