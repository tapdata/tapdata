package com.tapdata.tm.task.entity;


import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.message.constant.Level;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@Document("TaskDagCheckLog")
public class TaskDagCheckLog extends BaseEntity {
    private String taskId;
    private String nodeId;
    private String checkType;
    private String log;
    private Level grade;
}
