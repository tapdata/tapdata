package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.task.dto.TaskNodeRuntimeInfoDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

/**
 * SubTask
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("TaskNodeRuntimeInfo")
public class TaskNodeRuntimeInfoEntity extends BaseEntity {
    /** 任务id */
    private ObjectId taskId;
    /** nodeID */
    private String nodeId;
    /** 结构迁移 */
    private TaskNodeRuntimeInfoDto.StructureMigration structureMigration;
    /** 全量同步 */
    private TaskNodeRuntimeInfoDto.FullSync fullSync;
    /** 子任务名称 */
    private Map<String, Object> attrs;
}
