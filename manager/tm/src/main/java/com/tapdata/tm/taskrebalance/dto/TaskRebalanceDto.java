package com.tapdata.tm.taskrebalance.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
public class TaskRebalanceDto extends BaseDto {
    public static final String FIELD_CUSTOM_ID = "customId";
    public static final String FIELD_USER_ID = BaseDto.FIELD_USER_ID;
    public static final String FIELD_CREATE_TIME = "createTime";
    public static final String FIELD_NAME = "name";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_FINISH_AT = "finishAt";
    public static final String FIELD_TOTAL_COUNT = "totalCount";
    public static final String FIELD_PENDING_COUNT = "pendingCount";
    public static final String FIELD_STOPPING_COUNT = "stoppingCount";
    public static final String FIELD_STARTING_COUNT = "startingCount";
    public static final String FIELD_OK_COUNT = "okCount";
    public static final String FIELD_FAILED_COUNT = "failedCount";
    public static final String FIELD_CANCELLED_COUNT = "cancelledCount";
    public static final String FIELD_ERROR_MESG = "errorMesg";
    public static final String FIELD_EXECUTE_OWNER = "executeOwner";
    public static final String FIELD_IS_ACTIVED = "isActived";

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
