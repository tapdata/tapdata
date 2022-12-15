package io.tapdata.connector.redis.constant;

import org.apache.commons.lang3.StringUtils;

public enum ValueDataEnum {

    TEXT("Text"),
    JSON("Json"),
    ;

    private final String type;

    ValueDataEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static ValueDataEnum fromString(String type) {
        if (StringUtils.isBlank(type)) {
            return JSON;
        }
        for (ValueDataEnum value : values()) {
            if (type.equals(value.getType())) {
                return value;
            }
        }
        return JSON;
    }
}
