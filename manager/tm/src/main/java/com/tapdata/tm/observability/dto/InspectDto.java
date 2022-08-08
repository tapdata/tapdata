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
@Schema(description = "任务数据校验Dto")
public class InspectDto {
    @Schema(description = "搜索关键字")
    private String keyword;
    @Schema(description = "偏移量 默认值100")
    private int offset;
    @Schema(description = "数量，默认100")
    private int limit;
}
