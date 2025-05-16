package com.tapdata.tm.taskinspect.exception;

import lombok.Getter;

/**
 * 任务内校验异常-找不到表字段映射
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/11 21:03 Create
 */
public class TaskInspectNotFoundTableFieldMappingException extends TaskInspectException {
    @Getter
    private final String tableName;

    public TaskInspectNotFoundTableFieldMappingException(String taskId, String tableName) {
        super(String.format("not found table '%s' fields mapping", tableName),taskId);
        this.tableName = tableName;
    }
}
