package com.tapdata.tm.shareCdcTableMetrics;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


/**
 * ShareCdcTableMetrics
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ShareCdcTableMetricsDto extends BaseDto {
    private String taskId;
    private String nodeId;
    private String connectionId;
    private String connectionName;
    private Long startCdcTime;
    private Long firstEventTime;
    private Long currentEventTime;
    private Long count;
    private Long allCount;
}