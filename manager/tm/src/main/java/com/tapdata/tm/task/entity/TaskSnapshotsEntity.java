package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.task.dto.TaskDatabaseRuntimeInfoDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskNodeRuntimeInfoDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * SubTask
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("TaskSnapshots")
public class TaskSnapshotsEntity extends BaseEntity {
    /** 任务id */
    @Indexed
    private ObjectId taskId;
    /** 任务 */
    private TaskDto snapshot;
    /** 运行时信息*/
    private TaskNodeRuntimeInfoDto nodeRunTimeInfo;
    private TaskDatabaseRuntimeInfoDto DatabaseRunNodeTimeInfo;
}
