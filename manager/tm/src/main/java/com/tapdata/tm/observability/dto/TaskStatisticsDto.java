package com.tapdata.tm.observability.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Schema(description = "任务事件统计Dto")
public class TaskStatisticsDto {
    @Schema(description = "任务id")
    private String taskId;
    @Schema(description = "统计周期")
    private String statisticsCycle;
    @Schema(description = "性能指标周期")
    private String indexCycle;
    @Schema(description = "开始时间-时间戳")
    private Long startTime;
    @Schema(description = "结束时间-时间戳")
    private Long endTime;
}
