package com.tapdata.tm.modules.emuns;

import lombok.Getter;

@Getter
public enum ParamType {
    NUMBER("number"),
    STRING("string"),
    DATE("date"),
    DATE_TIME("datetime"),
    TIME("time"),
    BOOLEAN("boolean"),
    ARRAY("array"),
    OBJECT("object");

    final String type;
    ParamType(String type) {
        this.type = type;
    }
}
