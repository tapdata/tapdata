package com.tapdata.tm.autoinspect.constants;

import com.tapdata.tm.autoinspect.exception.AutoInspectException;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/4 15:27 Create
 */
public enum TaskType {
    Initial, Increment, InitialAndIncrement,
    ;

    public boolean hasInitial() {
        return this == InitialAndIncrement || this == Initial;
    }

    public boolean hasIncrement() {
        return this == InitialAndIncrement || this == Increment;
    }

    public static TaskType parseByTaskType(String taskSyncType) {
        if (ParentTaskDto.TYPE_INITIAL_SYNC.equals(taskSyncType)) {
            return Initial;
        } else if (ParentTaskDto.TYPE_CDC.equals(taskSyncType)) {
            return Increment;
        } else if (ParentTaskDto.TYPE_INITIAL_SYNC_CDC.equals(taskSyncType)) {
            return InitialAndIncrement;
        }
        throw AutoInspectException.notFoundSyncType(taskSyncType);
    }
}
