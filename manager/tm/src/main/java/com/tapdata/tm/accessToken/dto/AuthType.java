package com.tapdata.tm.accessToken.dto;

public enum AuthType {
    ACCESS_CODE("access_code")  ;

    private final String value;

    // 构造方法
    private AuthType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
