package io.tapdata.connector.redis.constant;

import org.apache.commons.lang3.StringUtils;

public enum ValueTypeEnum {

    STRING("String"),
    LIST("List"),
    HASH("Hash"),
    SET("Set"),
    ZSET("ZSet"),
    ;

    private final String type;

    ValueTypeEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static ValueTypeEnum fromString(String type) {
        if (StringUtils.isBlank(type)) {
            return STRING;
        }
        for (ValueTypeEnum value : values()) {
            if (type.equals(value.getType())) {
                return value;
            }
        }
        return STRING;
    }
}
