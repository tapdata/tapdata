package com.tapdata.tm.task.constant;

import java.util.ArrayList;
import java.util.List;

public enum TaskEnum {
    STATUS_EDIT("edit"),
    //STATUS_PREPARING("preparing"),
    STATUS_WAIT_START("wait_start"),
    STATUS_SCHEDULING("scheduling"),
    STATUS_SCHEDULE_FAILED("schedule_failed"),
    STATUS_WAIT_RUN("wait_run"),
    STATUS_RUNNING("running"),
    STATUS_STOPPING("stopping"),
    STATUS_PAUSING("pausing"),
    STATUS_ERROR("error"),
    STATUS_COMPLETE("complete"),

    STATUS_STOP("stop");


    private final String value;

    // 构造方法
    private TaskEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static List<String> getAllStatus() {
        List<String> allStatus = new ArrayList();
        //循环输出 值
        for (TaskEnum e : TaskEnum.values()) {
//            System.out.println(e.toString());
            allStatus.add(e.getValue());
        }
        return allStatus;
    }

}
