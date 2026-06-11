package com.tapdata.tm.taskrebalance.vo;

import lombok.Data;

import java.util.Date;

@Data
public class TaskRebalanceJobVo {
    private String id;
    private String customId;
    private Date createTime;
    private Date lastUpdated;
    private String userId;
    private String lastUpdBy;
    private String createUser;
    private String rebalanceId;
    private String taskId;
    private String taskName;
    private String status;
    private String errorMesg;
    private String sourceAgentId;
    private String targetAgentId;
    private Date beginAt;
    private Date finishAt;
}
