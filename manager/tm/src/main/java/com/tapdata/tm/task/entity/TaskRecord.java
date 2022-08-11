package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@Data
@EqualsAndHashCode(callSuper = true)
@Document("task_record")
@NoArgsConstructor
public class TaskRecord extends BaseEntity {
    private String taskId;
    private TaskEntity taskSnapshot;

    public TaskRecord(String id, String taskId, TaskEntity taskSnapshot, String userId, Date createAt) {
        this.taskId = taskId;
        this.taskSnapshot = taskSnapshot;
        this.setId(MongoUtils.toObjectId(id));
        this.setUserId(userId);
        this.setCreateAt(createAt);
    }
}
