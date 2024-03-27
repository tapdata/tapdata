package com.tapdata.tm.inspect.constant;


public enum Mode {
    MANUAL("manual"),
    CRON("cron")  ;

    private final String value;

    // 构造方法
    private Mode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
