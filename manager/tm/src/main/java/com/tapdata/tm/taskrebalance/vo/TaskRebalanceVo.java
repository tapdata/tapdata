package com.tapdata.tm.taskrebalance.vo;

import lombok.Data;

import java.util.Date;

@Data
public class TaskRebalanceVo {
    private String id;
    private String customId;
    private Date createTime;
    private Date lastUpdated;
    private String userId;
    private String lastUpdBy;
    private String createUser;
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
}
