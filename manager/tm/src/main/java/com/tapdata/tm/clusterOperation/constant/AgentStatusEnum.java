package com.tapdata.tm.clusterOperation.constant;



public enum AgentStatusEnum {
    NEED_UPDATE(0),

    UPDATING(1),

    UPDATED_TIME_OUT(4);

    private Integer value;

    // 构造方法
    private AgentStatusEnum(Integer value) {
        this.value = value;
    }



    public Integer getValue() {
        return value;
    }
}
