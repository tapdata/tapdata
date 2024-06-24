package com.tapdata.tm.inspect.constant;

public enum InspectMethod {
    ROW_COUNT("row_count"),
    FIELD("field"),
    JOINTFIELD("jointField"),
    CDC_COUNT("cdcCount"),
    JUNIT_TEST("junitTest"),
    HASH("hash"),
    ;

    private final String value;

    // 构造方法
    private InspectMethod(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
