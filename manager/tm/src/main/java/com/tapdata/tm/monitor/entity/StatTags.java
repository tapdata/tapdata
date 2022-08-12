package com.tapdata.tm.monitor.entity;

import lombok.Data;

/**
 * @Author: Zed
 * @Date: 2022/3/15
 * @Description:
 */
@Data
public class StatTags {
    private String agentId;
    private String nodeId;
    private String taskId;
    private String type;
}
