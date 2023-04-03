package com.tapdata.tm.task.constant;


public enum TableStatusEnum {
    STATUS_DRAFT("draft"),
    STATUS_NORMAL("normal"),
    STATUS_ERROR("error");

    private final String value;

    // 构造方法
    private TableStatusEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }





}
