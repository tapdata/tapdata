package io.tapdata.utils;

/**
 * 文件路径管理
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/10/31 17:22 Create
 */
public interface VfsFilepath {

    static String task_recoverSql_inspect(String taskId, String inspectResultId) {
        return String.format("recover-sql/inspect/%s-%s.sql", taskId, inspectResultId);
    }

    static String task_recoverSql_inspect_downloadName(String inspectResultId) {
        return String.format("inspect-recover-sql-%s.sql", inspectResultId);
    }

    static String task_recoverSql_taskInspect(String taskId, String manualId) {
        return String.format("recover-sql/task-inspect/%s-%s.sql", taskId, manualId);
    }

    static String task_recoverSql_taskInspect_downloadName(String manualId) {
        return String.format("task-inspect-recover-sql-%s.sql", manualId);
    }

}
