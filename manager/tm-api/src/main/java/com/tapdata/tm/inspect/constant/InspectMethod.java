package com.tapdata.tm.inspect.constant;

/** Supported methods: row count, field, joint field, CDC count, JUnit, hash, and custom query. */
public enum InspectMethod {
    ROW_COUNT("row_count"),
    FIELD("field"),
    JOINTFIELD("jointField"),
    CDC_COUNT("cdcCount"),
    JUNIT_TEST("junitTest"),
    HASH("hash"),
    CUSTOM_QUERY("custom_query")
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
