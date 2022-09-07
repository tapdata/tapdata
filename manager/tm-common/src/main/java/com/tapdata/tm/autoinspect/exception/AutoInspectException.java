package com.tapdata.tm.autoinspect.exception;

import com.tapdata.tm.autoinspect.constants.TaskStatus;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.entity.CompareTableItem;

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

    public static AutoInspectException canNotOpen(String taskName) {
        return new AutoInspectException(String.format("Can not open %s: %s", AutoInspectConstants.MODULE_NAME, taskName));
    }

    public static AutoInspectException openAutoDDL(String taskName) {
        return new AutoInspectException(String.format("Turn on automatic DDL to ignore %s: %s", AutoInspectConstants.MODULE_NAME, taskName));
    }

    public static AutoInspectException enableDynamicTable(String taskName, String tableName) {
        return new AutoInspectException(String.format("Enable dynamic table to ignore %s: %s.%s", AutoInspectConstants.MODULE_NAME, taskName, tableName));
    }

    public static AutoInspectException filterSourceNode(String targetNodeName, String msg) {
        return new AutoInspectException(String.format("%s '%s' filter source node: %s", AutoInspectConstants.MODULE_NAME, targetNodeName, msg));
    }

    public static AutoInspectException notSourceNode(String taskId) {
        return new AutoInspectException(String.format("%s '%s' not found any source node", AutoInspectConstants.MODULE_NAME, taskId));
    }

    public static AutoInspectException notEmptyTables(String taskId) {
        return new AutoInspectException(String.format("%s '%s' not found any tables", AutoInspectConstants.MODULE_NAME, taskId));
    }

    public static AutoInspectException startError(String taskId, Throwable cause) {
        return new AutoInspectException(String.format("%s '%s' start failed: %s", AutoInspectConstants.MODULE_NAME, taskId, cause.getMessage()), cause);
    }

    public static AutoInspectException existRunner(String taskId, TaskStatus.Sync syncStatus, TaskStatus.Inspect inspectStatus) {
        return new AutoInspectException(String.format("%s '%s' exists runner{sync:%s, inspect:%s}", AutoInspectConstants.MODULE_NAME, taskId, syncStatus, inspectStatus));
    }

    public static AutoInspectException notFoundSyncType(String taskSyncType) {
        throw new AutoInspectException(String.format("not parse task syncType: %s", taskSyncType));
    }

    public static AutoInspectException diffMaxSize(long maxSize, CompareTableItem tableItem) {
        throw new AutoInspectException(String.format("The maximum number %d of table '%s' differences has been reached", maxSize, tableItem.getTableName()));
    }
}
