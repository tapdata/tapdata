package com.tapdata.tm.message.constant;

/**
 * 消息来源模块名称
 * MessageType: 'connection/dataFlow/agent/tapdata_agent',
 */
public enum SourceModuleEnum {
    CONNECTION("connection"),
    AGENT("agent"),
    TAPDATA_AGENT("tapdata_agent"),
    DATAFLOW("dataFlow");

    private final String value;

    // 构造方法
    private SourceModuleEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }


    public String getByName(String name) {
        for (SourceModuleEnum s : SourceModuleEnum.values()) {
            if (s.getValue().equals(name)) {
                return s.getValue();
            }
        }
        return "";
    }

}
