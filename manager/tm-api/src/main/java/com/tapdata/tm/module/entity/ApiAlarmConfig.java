package com.tapdata.tm.module.entity;

import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleVO;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/16 14:06 Create
 * @description
 */
@Data
public class ApiAlarmConfig {
    private List<AlarmSettingVO> alarmSettings;

    private List<AlarmRuleVO> alarmRules;

    private List<String> emailReceivers;
}
