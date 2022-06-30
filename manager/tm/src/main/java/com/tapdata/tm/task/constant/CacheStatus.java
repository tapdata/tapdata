package com.tapdata.tm.task.constant;

import com.tapdata.tm.commons.task.dto.SubTaskDto;

public enum CacheStatus {
    READY("ready"),

    ERROR("error"),

    RUNNING("running");

    private final String value;

    // 构造方法
    private CacheStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }


    public static String getMapStatus(String status) {
        String cacheStatus = "";
        switch (status) {
            case SubTaskDto.STATUS_EDIT:
            case SubTaskDto.STATUS_SCHEDULING:
            case SubTaskDto.STATUS_COMPLETE:
            case SubTaskDto.STATUS_WAIT_RUN:
                cacheStatus = READY.getValue();
                break;

            case SubTaskDto.STATUS_STOPPING:
            case SubTaskDto.STATUS_STOP:
            case SubTaskDto.STATUS_SCHEDULE_FAILED:
            case SubTaskDto.STATUS_ERROR:
                cacheStatus = ERROR.getValue();
                break;
            case SubTaskDto.STATUS_RUNNING:
                cacheStatus = RUNNING.getValue();
                break;
        }
        return cacheStatus;
    }


}
