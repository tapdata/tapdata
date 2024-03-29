package com.tapdata.tm.alarmrule.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Getter
@Setter
public class UpdateRuleDto {
    private String key;
    private boolean notify;
}
