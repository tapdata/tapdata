package com.tapdata.tm.taskinspect;

import io.tapdata.exception.TapRuntimeException;
import lombok.Getter;

/**
 * 任务校验-异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 17:09 Create
 */
@Getter
public class TaskInspectException extends TapRuntimeException {

    public static final int CODE_DUPLICATE_INS = 1;
    public static final int CODE_UN_SUPPORT_MODE = 2;
    public static final int CODE_MODE_ILLEGAL = 3;
    public static final int CODE_UNINITIALIZED = 4;

    private final String taskId;
    private final int code;

    public TaskInspectException(String taskId, int code) {
        this.taskId = taskId;
        this.code = code;
    }

    public TaskInspectException(String message, String taskId, int code) {
        super(message);
        this.taskId = taskId;
        this.code = code;
    }

//    public TaskInspectException(String message, Throwable cause, String taskId, int code) {
//        super(message, cause);
//        this.taskId = taskId;
//        this.code = code;
//    }

    public TaskInspectException(Throwable cause, String taskId, int code) {
        super(cause);
        this.taskId = taskId;
        this.code = code;
    }


    /**
     * 不能创建重复实例
     *
     * @param taskId 任务编号
     * @return 异常
     */
    public static TaskInspectException cannotBeDuplicated(String taskId) {
        return new TaskInspectException(taskId, CODE_DUPLICATE_INS);
    }

    /**
     * 任务内校验未初始化
     *
     * @param taskId 任务编号
     * @return 异常
     */
    public static TaskInspectException uninitialized(String taskId) {
        return new TaskInspectException("not found task-inspect instance", taskId, CODE_UNINITIALIZED);
    }

    /**
     * 非法的校验模式
     *
     * @param taskId 任务编号
     * @param mode   校验模式
     * @return 异常
     */
    public static TaskInspectException illegalMode(String taskId, TaskInspectMode mode) {
        return new TaskInspectException(String.format("illegal mode for task '%s'", mode), taskId, CODE_MODE_ILLEGAL);
    }

    /**
     * 不支持的校验方式
     *
     * @param taskId 任务
     * @param mode   校验方式
     * @return 异常
     */
    public static TaskInspectException unSupportMode(String taskId, TaskInspectMode mode) {
        return new TaskInspectException("un-support mode: " + mode, taskId, CODE_UN_SUPPORT_MODE);
    }

    public static TaskInspectException stopTimeout(String taskId, String jobId, long timeout) {
        return new TaskInspectException(String.format("stop job '%s' timeout, %dms", jobId, timeout), taskId, CODE_UN_SUPPORT_MODE);
    }
}
