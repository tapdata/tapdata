package com.tapdata.tm.task.constant;


/**
 *  migrate   即数据复制
 *  sync    即数据开发
 * logCollector 挖掘任务
 */
public enum SyncType {
    SYNC("sync"),  // 即数据开发

    MIGRATE("migrate"),  //即数据复制

    LOG_COLLECTOR("logCollector");

    private final String value;

    // 构造方法
    private SyncType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
