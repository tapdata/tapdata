package com.tapdata.tm.task.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
public class RelationTaskInfoVo {
    private String id;
    private String name;
    @Schema(description = "任务类型 logCollector shareCache inspect")
    private String type;
    private String syncType;
    private String taskType;
    @Schema(description = "任务状态 同taskStatus")
    private String status;
    private Long startTime;
    private Long currentEventTimestamp;
    private long tableNum;
    private Date createDate;
    
}
