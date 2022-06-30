package com.tapdata.tm.userLog.constant;

public enum UserLogType {
    USER_OPERATION("userOperation");

    private final String value;

    // 构造方法
    private UserLogType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
