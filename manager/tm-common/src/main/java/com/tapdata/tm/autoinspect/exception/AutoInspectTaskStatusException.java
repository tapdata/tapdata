package com.tapdata.tm.autoinspect.exception;

import com.tapdata.tm.autoinspect.constants.TaskStatus;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;

/**
 * 增量校验状态异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/5/10 14:41 Create
 */
public class AutoInspectTaskStatusException extends AutoInspectException {
    private AutoInspectTaskStatusException(String message) {
        super(message);
    }

    private AutoInspectTaskStatusException(String taskId, String message) {
        super(String.format("%s '%s' %s", AutoInspectConstants.MODULE_NAME, taskId, message));
    }

    public static void notChangeWithInspectError(TaskStatus.Sync changeStatus, String errMsg) {
        throw new AutoInspectTaskStatusException(String.format("%s not change sync status to %s error: %s", AutoInspectConstants.MODULE_NAME, changeStatus, errMsg));
    }

    public static void notChangeWithSyncError(TaskStatus.Inspect changeStatus, String errMsg) {
        throw new AutoInspectTaskStatusException(String.format("%s not change inspect status to %s error: %s", AutoInspectConstants.MODULE_NAME, changeStatus, errMsg));
    }

    public static void notInspectError(TaskStatus.Inspect status, String err) {
        if (TaskStatus.Inspect.Error == status) {
            throw new AutoInspectTaskStatusException("", err);
        }
    }

    public static AutoInspectTaskStatusException notSyncError(String err) {
        return new AutoInspectTaskStatusException(err);
    }
}
