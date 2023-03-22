package com.tapdata.tm.livedataplatform.constant;

public enum ClusterEnum {
    ATLAS_CLUSTER("atlas"),
    SELF_CLUSTER("self")
    ;

    private final String value;

    // 构造方法
    private ClusterEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}

