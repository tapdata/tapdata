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
@Schema(description = "并行请求响应VO")
public class ParallelInfoVO<T> {
    @Schema(description = "请求处理的代码", defaultValue = "ok")
    private String code;
    @Schema(description = "请求处理失败时的错误消息")
    private String error;
    @Schema(description = "范型 例子TaskStatisticsVO等可观测性类")
    private T data;
}
