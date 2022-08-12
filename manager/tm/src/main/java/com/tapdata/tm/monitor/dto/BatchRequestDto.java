package com.tapdata.tm.monitor.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Schema(description = "批处理请求参数dto Map格式 {k:v}")
public class BatchRequestDto extends HashMap<String, Object> {
}
