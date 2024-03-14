package com.tapdata.tm.inspect.constant;

public enum InspectMethod {
    FIELD("field"),
    ROW_COUNT("row_count")  ;

    private final String value;

    // 构造方法
    private InspectMethod(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
