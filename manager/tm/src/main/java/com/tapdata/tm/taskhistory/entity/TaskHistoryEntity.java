package com.tapdata.tm.taskhistory.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;


/**
 * TaskHistory
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("TaskHistories")
public class TaskHistoryEntity extends BaseEntity {
    private String task_id;
    private String task_name;

    private int task_result_code;

    private Object task_result;

    private long task_duration;

    private Date task_start_time;

    private String agent_id;

}