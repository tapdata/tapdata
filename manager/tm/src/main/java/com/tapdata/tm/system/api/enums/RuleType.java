package com.tapdata.tm.system.api.enums;

import lombok.Getter;

public enum RuleType {
    SYSTEM(999, "Built in system"),
    USER(0, "User defined")
    ;
    @Getter
    final
    int code;
    final String description;
    RuleType(int code, String description) {
        this.code = code;
        this.description = description;
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
