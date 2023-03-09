package com.tapdata.tm.monitor.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.Map;

@Data
@Schema(description = "批请求 uri param dto类")
public class BatchUriParamDto {
    @Schema(description = "请求uri，/api/task-console")
    private String uri;
    @Schema(description = "请求参数")
    private Map param;
}
