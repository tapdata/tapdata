package com.tapdata.tm.alarm.dto;

import lombok.Builder;
import lombok.ToString;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@ToString
@Builder
public class TaskAlarmNodeInfoVo {
    private String nodeId;
    private String nodeName;
    private Integer num;
}
