package com.tapdata.tm.task.bean;

import com.tapdata.tm.commons.task.constant.Level;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;


/**
 * 任务重置推送给前端的日志
 */
@Data
@Document("TaskResetLogs")
public class TaskResetLogs {
    @Id
    private String id;
    private String taskId;
    private Level level;
    private Date time;
    private String description;

}
