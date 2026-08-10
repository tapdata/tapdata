package com.tapdata.tm.commons.schema.enums;

import lombok.Getter;

@Getter
public enum TableFieldTag {
    USER_CREATE("USER_CREATE", "Field create by user"),
    SCHEMA_LOAD("SCHEMA_LOAD", "field from discover schema load"),
    LOGIC_LOAD("LOGIC_LOAD", "field from logic schema load"),
    NORMAL("NORMAL", "-"),
    ;
    final String type;
    final String desc;

    TableFieldTag(String type, String desc) {
        this.desc = desc;
        this.type = type;
    }
}
