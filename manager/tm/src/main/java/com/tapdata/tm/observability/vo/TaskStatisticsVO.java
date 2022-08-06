package com.tapdata.tm.observability.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Schema(description = "任务事件统计VO")
public class TaskStatisticsVO {
    @Schema(description = "任务id")
    private String taskId;
    @Schema(description = "任务名称")
    private String name;
    @Schema(description = "任务类型")
    private String syncType;
    @Schema(description = "运行状态")
    private String status;
    @Schema(description = "统计周期")
    private String statisticsCycle;
    @Schema(description = "性能指标周期")
    private String indexCycle;
}
