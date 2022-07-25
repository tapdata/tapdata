package com.tapdata.tm.task.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "日志VO")
public class TaskLogInfoVo {
    @Schema(description = "id")
    private String id;
    @Schema(description = "等级 INFO WARN ERROR")
    private String grade;
    @Schema(description = "日志文本")
    private String log;
}
