package com.tapdata.tm.alarmrule.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Getter
@Setter
@AllArgsConstructor
public class AlarmRuleDto {
    private String key;
    private int point;
    @Schema(description = "-1 小于等于 <= ; 0; 1 大于等于 >= ")
    private int equalsFlag;
    @Schema(description = "毫秒")
    private int ms;
}
