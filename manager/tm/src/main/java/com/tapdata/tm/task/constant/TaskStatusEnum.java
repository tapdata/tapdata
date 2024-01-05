package com.tapdata.tm.task.constant;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public enum TaskStatusEnum {
    STATUS_EDIT("edit"),
    STATUS_SCHEDULING("scheduling"),
    STATUS_SCHEDULE_FAILED("schedule_failed"),
    STATUS_WAIT_RUN("wait_run"),
    STATUS_RUNNING("running"),
    STATUS_STOPPING("stopping"),
    STATUS_PAUSING("pausing"),
    STATUS_PAUSED("paused"),
    STATUS_ERROR("error"),
    STATUS_COMPLETE("complete"),
    STATUS_STOP("stop"),
    STATUS_WAIT_START("wait_start");

    private final String value;

    // 构造方法
    private TaskStatusEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }


    public static List<String> getAllStatus() {
        List<String> allStatus = new ArrayList();
        //循环输出 值
        for (TaskStatusEnum e : TaskStatusEnum.values()) {
//            System.out.println(e.toString());
            allStatus.add(e.getValue());
        }
        return allStatus;
    }

    /**
     *   https://tapdata.feishu.cn/docs/doccnMWXodMfSQXr6R8sOzIoWOg
     *
     * @param status
     * @return
     */
    public static String getMapStatus(String status) {
        String cacheStatus = status;
        if (STATUS_SCHEDULE_FAILED.getValue().equals(status)) {
            cacheStatus = STATUS_ERROR.getValue();
        }
        else if (STATUS_PAUSED.getValue().equals(status)) {
            cacheStatus = STATUS_STOP.getValue();
        }
        return cacheStatus;
    }

}
