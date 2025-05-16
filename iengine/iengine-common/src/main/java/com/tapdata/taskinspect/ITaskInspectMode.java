package com.tapdata.taskinspect;

import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.TaskInspectMode;

import java.util.LinkedHashMap;

/**
 * 校验模式的实现接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/17 18:50 Create
 */
public interface ITaskInspectMode {

    ITaskInspectMode EMPTY = new ITaskInspectMode() {
    };

    default TaskInspectMode getMode() {
        return TaskInspectMode.CLOSE;
    }

    default void refresh(TaskInspectConfig config) throws InterruptedException {
    }

    /**
     * @param cdcReadTs 增量读取时间
     * @param cdcOpTs   增量变更时间
     * @param tableName 表名
     * @param keys      行主键
     */
    default void acceptCdcEvent(long cdcReadTs, long cdcOpTs, String tableName, LinkedHashMap<String, Object> keys) {

    }

    default boolean stop() {
        return true;
    }

}
