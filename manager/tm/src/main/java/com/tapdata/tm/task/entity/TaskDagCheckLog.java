package com.tapdata.tm.task.entity;


import com.tapdata.tm.base.entity.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Document("TaskDagCheckLog")
public class TaskDagCheckLog extends BaseEntity {
    private String taskId;
    private String nodeId;
    private String checkType;
    private String log;
    private String grade;
}
