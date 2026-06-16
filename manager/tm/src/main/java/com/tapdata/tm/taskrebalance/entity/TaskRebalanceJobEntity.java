package com.tapdata.tm.taskrebalance.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("TaskRebalanceJob")
public class TaskRebalanceJobEntity extends BaseEntity {
    private String rebalanceId;
    private String taskId;
    private String taskName;
    private String type;
    private String syncType;
    private String status;
    private String errorMesg;
    private String sourceAgentId;
    private String targetAgentId;
    private Date beginAt;
    private Date finishAt;
}
