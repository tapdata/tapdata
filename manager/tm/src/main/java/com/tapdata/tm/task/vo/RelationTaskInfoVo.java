package com.tapdata.tm.task.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RelationTaskInfoVo {
    private String id;
    private String name;
    @Schema(description = "任务类型 logCollector shareCache inspect")
    private String type;
    @Schema(description = "任务状态 同taskStatus")
    private String status;
    private Long startTime;
    
}
