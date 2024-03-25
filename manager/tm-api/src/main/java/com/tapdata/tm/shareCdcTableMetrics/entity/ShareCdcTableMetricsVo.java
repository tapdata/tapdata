package com.tapdata.tm.shareCdcTableMetrics.entity;

import lombok.Data;

@Data
public class ShareCdcTableMetricsVo {
    private Object id;
    private String taskId;
    private String nodeId;
    private String connectionId;
    private String connectionName;
    private String tableName;
    private Long startCdcTime;
    private Long firstEventTime;
    private Long currentEventTime;
    private Long count;
    private Long allCount;
}
