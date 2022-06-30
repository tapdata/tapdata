package com.tapdata.tm.taskhistory.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;


/**
 * TaskHistory
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TaskHistoryDto extends BaseDto {
    private String task_id;
    private String task_name;

    private int task_result_code;

    private Object task_result;

    private long task_duration;

    private Date task_start_time;

    private String agent_id;

}