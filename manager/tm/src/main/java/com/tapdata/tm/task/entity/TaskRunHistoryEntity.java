package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;


/**
 * SubTask
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("TaskRunHistory")
public class TaskRunHistoryEntity extends BaseEntity {
    /** 任务id */
    private ObjectId taskId;
    /** 子任务id */
    private ObjectId subTaskId;
    /** 子任务名称 */
    private String subTaskName;
    /** 动作  run 启动 stop 停止 */
    private String action;

}