package com.tapdata.tm.autoinspect.exception;

import com.tapdata.tm.autoinspect.constants.TaskStatus;
import com.tapdata.tm.autoinspect.constants.Constants;

/**
 * 增量校验基础异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/5/10 14:24 Create
 */
public class AutoInspectException extends RuntimeException {

    AutoInspectException() {
    }

    AutoInspectException(String message) {
        super(message);
    }

    AutoInspectException(String message, Throwable cause) {
        super(message, cause);
    }

    AutoInspectException(Throwable cause) {
        super(cause);
    }

    public static AutoInspectException notSourceNode(String taskId) {
        return new AutoInspectException(String.format("%s '%s' not found any source node", Constants.MODULE_NAME, taskId));
    }

    public static AutoInspectException notEmptyTables(String taskId) {
        return new AutoInspectException(String.format("%s '%s' not found any tables", Constants.MODULE_NAME, taskId));
    }

    public static AutoInspectException startError(String taskId, Throwable cause) {
        return new AutoInspectException(String.format("%s '%s' start failed: %s", Constants.MODULE_NAME, taskId, cause.getMessage()), cause);
    }

    public static AutoInspectException existRunner(String taskId, TaskStatus.Sync syncStatus, TaskStatus.Inspect inspectStatus) {
        return new AutoInspectException(String.format("%s '%s' exists runner{sync:%s, inspect:%s}", Constants.MODULE_NAME, taskId, syncStatus, inspectStatus));
    }

    public static AutoInspectException notFoundSyncType(String taskSyncType) {
        throw new AutoInspectException(String.format("not parse task syncType: %s", taskSyncType));
    }
}
