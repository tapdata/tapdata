package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.utils.MongoUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@Document("TaskRecord")
@NoArgsConstructor
public class TaskRecord extends BaseEntity {
    private String taskId;
    private TaskEntity taskSnapshot;
    private List<TaskStatusUpdate> statusStack;
    private Long inputTotal;
    private Long outputTotal;

    public TaskRecord(String id, String taskId, TaskEntity taskSnapshot, String userId, Date createAt) {
        this.taskId = taskId;
        this.taskSnapshot = taskSnapshot;
        this.setId(MongoUtils.toObjectId(id));
        this.setUserId(userId);
        this.setCreateAt(createAt);
    }

    @Data
    @AllArgsConstructor
    public static class TaskStatusUpdate implements Comparable<TaskStatusUpdate> {
        String status;
        Date timestamp;


        @Override
        public int compareTo(@NotNull TaskStatusUpdate o) {
            return this.timestamp.compareTo(o.getTimestamp());
        }
    }
}
