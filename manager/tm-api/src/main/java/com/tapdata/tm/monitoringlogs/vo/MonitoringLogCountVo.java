package com.tapdata.tm.monitoringlogs.vo;

import lombok.Data;

/**
 * @author Dexter
 */
@Data
public class MonitoringLogCountVo {
    private String nodeId;
    private String level;
    private Integer count;
}
