package com.tapdata.tm.taskrebalance.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("TaskRebalance")
public class TaskRebalanceEntity extends BaseEntity {
    private String name;
    private String status;
    private Date finishAt;
    private Integer totalCount;
    private Integer pendingCount;
    private Integer stoppingCount;
    private Integer startingCount;
    private Integer okCount;
    private Integer failedCount;
    private Integer cancelledCount;
    private String errorMesg;
    private String executeOwner;
    private Boolean isActived;
}
