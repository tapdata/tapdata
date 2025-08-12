package com.tapdata.tm.system.api.enums;

public enum RuleType {
    SYSTEM(999, "Built in system"),
    USER(0, "User defined")
    ;
    int code;
    String description;
    RuleType(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public static RuleType getDescription(Integer code) {
        if (null == code) {
            return SYSTEM;
        }
        for (RuleType value : values()) {
            if (value.getCode() == code) {
                return value;
            }
        }
        return SYSTEM;
    }

    public static RuleType of(Integer e) {
        if (null == e) {
            return null;
        }
        for (RuleType value : values()) {
            if (value.code == e) {
                return value;
            }
        }
        return null;
    }
}
