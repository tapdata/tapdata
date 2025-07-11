package com.tapdata.tm.taskinspect.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 任务内校验-差异操作记录
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/9 21:58 Create
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class TaskInspectResultsOpDto extends BaseDto {

    public static final String OP_MANUAL_CHECK = "MANUAL_CHECK";
    public static final String OP_MANUAL_RECOVER = "MANUAL_RECOVER";

    public static final String FIELD_OP = "op";
    public static final String FIELD_OP_ID = "opId";
    public static final String FIELD_TASK_ID = "taskId";
    public static final String FIELD_CONFIG = "config";
    public static final String FIELD_TOTALS = "totals";

    private String op;
    private String opId;
    private String taskId;
    private String config;
    private Long totals;
}
