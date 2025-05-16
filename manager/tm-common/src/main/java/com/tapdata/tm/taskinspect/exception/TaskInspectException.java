package com.tapdata.tm.taskinspect.exception;

import lombok.Getter;

/**
 * 任务内校验异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/11 21:03 Create
 */
public class TaskInspectException extends Exception {
    @Getter
    private final String taskId;

    public TaskInspectException(String taskId) {
        this.taskId = taskId;
    }

    public TaskInspectException(String message, String taskId) {
        super(message);
        this.taskId = taskId;
    }

    public TaskInspectException(String message, Throwable cause, String taskId) {
        super(message, cause);
        this.taskId = taskId;
    }

    public TaskInspectException(Throwable cause, String taskId) {
        super(cause);
        this.taskId = taskId;
    }
}
