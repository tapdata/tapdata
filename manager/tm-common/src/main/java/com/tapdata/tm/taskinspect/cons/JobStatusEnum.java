package com.tapdata.tm.taskinspect.cons;

import lombok.Getter;

/**
 * 工作线程状态
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/25 10:43 Create
 */
@Getter
public enum JobStatusEnum {
    NONE(true),
    STARTING(false),
    START_ERROR(true),
    RUNNING(false),
    STOPPING(false),
    STOP_ERROR(true),
    STOPPED(true),
    ;

    private final boolean stopped;

    JobStatusEnum(boolean stopped) {
        this.stopped = stopped;
    }

}
