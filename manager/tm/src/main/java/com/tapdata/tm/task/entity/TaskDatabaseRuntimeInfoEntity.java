package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
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
@Document("TaskDatabaseRuntimeInfo")
public class TaskDatabaseRuntimeInfoEntity extends BaseEntity {
    /** 任务id */
    private ObjectId taskId;
    /** nodeID */
    private String nodeId;
    /** 源库id */
    private String sourceId;
    /** 源库名称 */
    private String sourceName;
    /** 目标库id */
    private String targetId;
    /** 目标库名称 */
    private String targetName;
    /** 延迟时间 */
    private Long delay;
    /** 当前同步到的时间点 */
    private Long currentTime;
    /** 子任务名称 */
    private Map<String, Object> attrs;
}
