package com.tapdata.tm.task.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Schema(description = "js模型推演试运行结果Dto")
public class JsResultDto {
    private String code;
    private String message;
    private String taskId;
    private List<Map<String, Object>> before;
    private List<Map<String, Object>> after;
    private Long version;
}
