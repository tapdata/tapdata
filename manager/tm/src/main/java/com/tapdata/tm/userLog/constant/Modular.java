package com.tapdata.tm.userLog.constant;

/**
 * 随着业务增加，可以新增新的枚举值
 * 这里需要和国际化的资源属性文件里的key对应，
 */
public enum Modular {
    SYNC("sync"),
    MIGRATION("migration"),
    CONNECTION("connection"),
    AGENT("agent"),
    INSPECT("inspect"),
    MESSAGE("message"),
    USER_NOTIFICATION("userNotification"),


    DATA_FLOW_INSIGHT("DataFlowInsight"),
//    DATA_FLOWS("dataflows"),
    JOBS("Jobs"),
    METADATA_INSTANCES("MetadataInstances"),
    WORKERS("Workers"),
    SYSTEM("system"),
    USER("user");

    private final String value;

    // 构造方法
    private Modular(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
