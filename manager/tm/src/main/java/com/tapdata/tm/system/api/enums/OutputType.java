package com.tapdata.tm.system.api.enums;

public enum OutputType {
    AUTO(0, "自动识别，保持原始字符长度"),
    CUSTOM(1, "将识别出的连续字符根据配置替换成指定长度")

    ;
    int code;
    String description;
    OutputType(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public static OutputType by(int code) {
        var values = values();
        for (OutputType value : values) {
            if (value.getCode() == code) {
                return value;
            }
        }
        return AUTO;
    }
}
