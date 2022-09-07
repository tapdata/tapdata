package com.tapdata.tm.alarm.dto;

import lombok.Builder;
import lombok.Data;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@Builder
@Data
public class TaskAlarmNodeInfoVo {
    private String nodeId;
    private String nodeName;
    private Integer num;
}
