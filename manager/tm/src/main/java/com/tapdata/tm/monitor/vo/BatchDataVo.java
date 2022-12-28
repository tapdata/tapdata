package com.tapdata.tm.monitor.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BatchDataVo {
    @Schema(description = "请求处理的代码", defaultValue = "ok")
    private String code;
    @Schema(description = "请求处理失败时的错误消息")
    private String error;
    @Schema(description = "范型 例子TaskStatisticsVO等可观测性类")
    private Object data;

    private Long cost;
}
