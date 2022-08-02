package com.tapdata.tm.commons.task.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * SubTask
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TaskSnapshotsDto extends BaseDto {
    /** 任务id */
    private ObjectId taskId;
    /** 任务 */
    private TaskDto snapshot;
    /** 运行时信息*/
    private TaskNodeRuntimeInfoDto nodeRunTimeInfo;
    private TaskDatabaseRuntimeInfoDto DatabaseRunNodeTimeInfo;
}
