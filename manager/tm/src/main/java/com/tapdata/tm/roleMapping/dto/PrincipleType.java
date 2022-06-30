package com.tapdata.tm.roleMapping.dto;

public enum PrincipleType {
    PERMISSION("PERMISSION"),
    USER("USER")  ;

    private final String value;

    // 构造方法
    private PrincipleType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
