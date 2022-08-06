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
@Schema(description = "批处理请求参数dto")
public class ServiceMethodDto<T> {
    @Schema(description = "服务名称")
    private String serviceName;

    @Schema(description = "方法名称")
    private String methodName;

    @Schema(description = "请求参数")
    private T param;
}
