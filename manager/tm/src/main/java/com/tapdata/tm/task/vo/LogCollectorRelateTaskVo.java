package com.tapdata.tm.task.vo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class LogCollectorRelateTaskVo {
    private String taskId;
    private String name;
    private String type;
    private String syncType;
    private String status;
    private Long creatTime;
}
