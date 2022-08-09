package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@EqualsAndHashCode(callSuper = true)
@Document("task_record")
public class TaskRecord extends BaseEntity {
    private String taskId;
    private TaskEntity taskSnapshot;
}
