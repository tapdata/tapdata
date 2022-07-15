package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;


/**
 * MonitoringLogs
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MonitoringLogsDto extends BaseDto {
    private String level;
    private Long timestamp;
    private Date date;
    private String taskId;
    private Integer version;
    private String message;
    private String errorStack;
}