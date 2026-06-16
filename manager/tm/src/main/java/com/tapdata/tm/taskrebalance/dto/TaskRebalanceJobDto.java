package com.tapdata.tm.taskrebalance.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
public class TaskRebalanceJobDto extends BaseDto {
    public static final String FIELD_CUSTOM_ID = "customId";
    public static final String FIELD_USER_ID = BaseDto.FIELD_USER_ID;
    public static final String FIELD_CREATE_TIME = "createTime";
    public static final String FIELD_REBALANCE_ID = "rebalanceId";
    public static final String FIELD_TASK_ID = "taskId";
    public static final String FIELD_TASK_NAME = "taskName";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_SYNC_TYPE = "syncType";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_ERROR_MESG = "errorMesg";
    public static final String FIELD_SOURCE_AGENT_ID = "sourceAgentId";
    public static final String FIELD_TARGET_AGENT_ID = "targetAgentId";
    public static final String FIELD_BEGIN_AT = "beginAt";
    public static final String FIELD_FINISH_AT = "finishAt";

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
