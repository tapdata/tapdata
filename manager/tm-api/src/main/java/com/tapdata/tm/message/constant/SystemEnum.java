package com.tapdata.tm.message.constant;


public enum SystemEnum {
    //数据迁移
    MIGRATION("migration"),
    //数据源同步
    SYNC("sync"),

    //数据源同步
    DATAFLOW("dataFlow"),


    //数据校验
    INSPECT("inspect"),

    AGENT("agent");

    // 成员变量
    private String value;

    // 构造方法
    private SystemEnum(String value) {
        this.value = value;
    }


    public String getByName(String name) {
        for (SystemEnum s : SystemEnum.values()) {
            if (s.getValue().equals(name)) {
                return s.getValue();
            }
        }
        return "";
    }

    public String getValue() {
        return value;
    }


    public static SystemEnum getEnumByValue(String value) {
        SystemEnum[] systemEnum = values();
        for (SystemEnum businessModeEnum : systemEnum) {
            if (businessModeEnum.getValue().equals(value)) {
                return businessModeEnum;
            }
        }
        return null;
    }


}
