package com.tapdata.tm.Settings.constant;

import com.tapdata.tm.schedule.job.TaskStatusErrorJob;
import com.tapdata.tm.schedule.job.TaskStatusStopJob;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum AlarmKeyEnum {
    TASK_STATUS_ERROR(TaskStatusErrorJob.class.getName()),
    TASK_INSPECT_ERROR(""),
    TASK_FULL_COMPLETE(""),
    TASK_INCREMENT_COMPLETE(""),
    TASK_STATUS_STOP(TaskStatusStopJob.class.getName()),
    TASK_INCREMENT_DELAY(""),

    DATANODE_CANNOT_CONNECT(""),
    DATANODE_HTTP_CONNECT_CONSUME(""),
    DATANODE_TCP_CONNECT_CONSUME(""),
    DATANODE_AVERAGE_HANDLE_CONSUME(""),

    PROCESSNODE_AVERAGE_HANDLE_CONSUME("");

    private final String className;
}
