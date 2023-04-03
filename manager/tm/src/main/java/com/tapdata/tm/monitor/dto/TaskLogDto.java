package com.tapdata.tm.monitor.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Schema(description = "任务日志Dto")
public class TaskLogDto {
    @Schema(description = "任务id")
    private String taskId;
    @Schema(description = "节点id，默认空，查全部节点日志")
    private String nodeId;
    @Schema(description = "搜索关键字")
    private String keyword;
    @Schema(description = "日志等级 DEBUG INFO WARN ERROR")
    private String grade;
    @Schema(description = "开始时间-时间戳")
    private Long startTime;
    @Schema(description = "结束时间-时间戳")
    private Long endTime;
    @Schema(description = "偏移量 默认值0")
    private int offset = 0;
    @Schema(description = "数量，默认100")
    private int limit = 100;

    private boolean startTask;
}
