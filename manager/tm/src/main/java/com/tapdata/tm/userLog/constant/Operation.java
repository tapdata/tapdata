package com.tapdata.tm.userLog.constant;

public enum Operation {
    UPDATE("update"),
    BATCH_UPDATE("batch_update"),

    CREATE ("create"),
    COPY("copy"),
    START("start"),
    STOP("stop"),
    FORCE_STOP("forceStop"),

    RENAME("rename"),
    PAUSE("pause"),

    RESET("reset"),
    BATCH_RESET("batch_reset"),


    READ_ALL("readAll"),
    READ("read"),
    LOGOUT("logout"),
    LOGIN("login"),


    DELETE_ALL("deleteAll"),
    DELETE("delete") ;

    private final String value;

    // 构造方法
    private Operation(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }


}
