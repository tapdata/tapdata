package com.tapdata.tm.alarm.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 告警统计：normal+warning级别的数量
 * 错误统计：critical+emergency级别的数量
 * @author jiuyetx
 * @date 2022/9/13
 */
@Data
@AllArgsConstructor
public class AlarmNumVo {
    private long alert;
    private long error;
}
