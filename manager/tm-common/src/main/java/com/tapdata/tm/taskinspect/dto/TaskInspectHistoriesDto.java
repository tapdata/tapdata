package com.tapdata.tm.taskinspect.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * 任务内校验-执行历史
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/9 21:58 Create
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class TaskInspectHistoriesDto extends BaseDto {

    public static final String FIELD_TASK_ID = "taskId";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_MESSAGE = "message";
    public static final String FIELD_CONFIG = "config";
    public static final String FIELD_CONFIG_ACCEPTS = "accepts";
    public static final String FIELD_CONFIG_IGNORES = "ignores";
    public static final String FIELD_CONFIG_DIFFERENCES = "differences";
    public static final String FIELD_ATTRS = "attrs";
    public static final String FIELD_PING_TIME = "pingTime";
    public static final String FIELD_BEGIN_TIME = "beginTime";
    public static final String FIELD_END_TIME = "endTime";

    private String taskId;
    private String type;
    private String status;
    private String message;
    private Map<String, Object> config;
    private Map<String, Object> attrs;
    private Long pingTime;
    private Long beginTime;
    private Long endTime;
}
