package com.tapdata.tm.clusterOperation.constant;


public enum SystemEnum {
    //数据迁移
    MIGRATION("migration"),

    //数据源同步
    AGENT("agent"),
    SYNC("sync");

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
}
