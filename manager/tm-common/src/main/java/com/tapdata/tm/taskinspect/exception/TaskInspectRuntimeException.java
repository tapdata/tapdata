package com.tapdata.tm.taskinspect.exception;

import com.tapdata.tm.taskinspect.TaskInspectMode;
import io.tapdata.exception.TapRuntimeException;
import lombok.Getter;

/**
 * 任务校验-异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 17:09 Create
 */
@Getter
public class TaskInspectRuntimeException extends TapRuntimeException {

    public static final int CODE_LOGIC_ERROR = 0;
    public static final int CODE_DUPLICATE_INS = 1;
    public static final int CODE_UN_SUPPORT_MODE = 2;
    public static final int CODE_MODE_ILLEGAL = 3;
    public static final int CODE_UNINITIALIZED = 4;

    private final String taskId;
    private final int code;

    public TaskInspectRuntimeException(String taskId, int code) {
        this.taskId = taskId;
        this.code = code;
    }

    public TaskInspectRuntimeException(String message, String taskId, int code) {
        super(message);
        this.taskId = taskId;
        this.code = code;
    }

//    public TaskInspectException(String message, Throwable cause, String taskId, int code) {
//        super(message, cause);
//        this.taskId = taskId;
//        this.code = code;
//    }

    public TaskInspectRuntimeException(Throwable cause, String taskId, int code) {
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
    public static TaskInspectRuntimeException cannotBeDuplicated(String taskId) {
        return new TaskInspectRuntimeException(taskId, CODE_DUPLICATE_INS);
    }

    /**
     * 任务内校验未初始化
     *
     * @param taskId 任务编号
     * @return 异常
     */
    public static TaskInspectRuntimeException uninitialized(String taskId) {
        return new TaskInspectRuntimeException("not found instance", taskId, CODE_UNINITIALIZED);
    }

    /**
     * 非法的校验模式
     *
     * @param taskId 任务编号
     * @param mode   校验模式
     * @return 异常
     */
    public static TaskInspectRuntimeException illegalMode(String taskId, TaskInspectMode mode) {
        return new TaskInspectRuntimeException(String.format("illegal mode for task '%s'", mode), taskId, CODE_MODE_ILLEGAL);
    }

    /**
     * 不支持的校验方式
     *
     * @param taskId 任务
     * @param mode   校验方式
     * @return 异常
     */
    public static TaskInspectRuntimeException unSupportMode(String taskId, TaskInspectMode mode) {
        return new TaskInspectRuntimeException("un-support mode: " + mode, taskId, CODE_UN_SUPPORT_MODE);
    }

    public static TaskInspectRuntimeException notSupportedOfferResult(String taskId, int step) {
        return new TaskInspectRuntimeException(String.format("not supported offer result '%s'", step), taskId, CODE_UN_SUPPORT_MODE);
    }

    public static TaskInspectRuntimeException logicError(String taskId, String msg) {
        return new TaskInspectRuntimeException("application logic error, need developer fixed: " + msg, taskId, CODE_LOGIC_ERROR);
    }
}
