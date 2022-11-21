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
    DELETE("delete"),

    UPDATE_PHONE("update_phone"),
    BIND_PHONE("bind_phone"),
    UPDATE_EMAIL("update_email"),
    BIND_EMAIL("bind_email"),
    RESET_PASSWORD("reset_password"),
    ;

    private final String value;

    // 构造方法
    private Operation(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static Operation of(String value) {
        Operation[] operations = Operation.values();
        for (int i = 0; i < operations.length; i++) {
            if(operations[i].value.equals(value)) {
                return operations[i];
            }
        }
        return Operation.UPDATE;
    }
}
