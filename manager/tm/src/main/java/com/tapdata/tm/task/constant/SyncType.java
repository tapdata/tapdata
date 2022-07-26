package com.tapdata.tm.task.constant;


import com.tapdata.tm.commons.task.dto.TaskDto;

/**
 *  migrate   即数据复制
 *  sync    即数据开发
 * logCollector 挖掘任务
 */
public enum SyncType {
    SYNC(TaskDto.SYNC_TYPE_SYNC),  // 即数据开发

    MIGRATE(TaskDto.SYNC_TYPE_MIGRATE),  //即数据复制

    LOG_COLLECTOR(TaskDto.SYNC_TYPE_LOG_COLLECTOR);

    private final String value;

    // 构造方法
    SyncType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
