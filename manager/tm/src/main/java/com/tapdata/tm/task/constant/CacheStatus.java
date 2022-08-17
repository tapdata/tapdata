package com.tapdata.tm.task.constant;


import com.tapdata.tm.commons.task.dto.TaskDto;

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
            case TaskDto.STATUS_EDIT:
            case TaskDto.STATUS_SCHEDULING:
            case TaskDto.STATUS_COMPLETE:
            case TaskDto.STATUS_WAIT_RUN:
                cacheStatus = READY.getValue();
                break;

            case TaskDto.STATUS_STOPPING:
            case TaskDto.STATUS_STOP:
            case TaskDto.STATUS_SCHEDULE_FAILED:
            case TaskDto.STATUS_ERROR:
                cacheStatus = ERROR.getValue();
                break;
            case TaskDto.STATUS_RUNNING:
                cacheStatus = RUNNING.getValue();
                break;
        }
        return cacheStatus;
    }


}
