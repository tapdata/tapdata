package com.tapdata.tm.commons.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
@AllArgsConstructor
public enum CapitalizedEnum {
    UPPER("toUpperCase"), // 转大写
    LOWER("toLowerCase"), // 转小写
    SNAKE("toSnakeCase"), // 转小写
    CAMEL("toCamelCase"), // 转小写
    NONE("");

    private final String value;

    public static CapitalizedEnum fromValue(String value) {
        for (CapitalizedEnum capitalizedEnum : CapitalizedEnum.values()) {
            if (StringUtils.equals(capitalizedEnum.getValue(), value)) {
                return capitalizedEnum;
            }
        }
        return NONE;
    }
}
