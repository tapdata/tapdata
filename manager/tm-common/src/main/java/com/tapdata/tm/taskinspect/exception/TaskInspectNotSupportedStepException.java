package com.tapdata.tm.taskinspect.exception;

import lombok.Getter;

/**
 * 任务内校验异常-未支持的校验步骤
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/11 21:03 Create
 */
public class TaskInspectNotSupportedStepException extends TaskInspectException {
    @Getter
    private final int step;

    public TaskInspectNotSupportedStepException(String taskId, int step) {
        super(String.format("not supported step '%s'", step), taskId);
        this.step = step;
    }
}
