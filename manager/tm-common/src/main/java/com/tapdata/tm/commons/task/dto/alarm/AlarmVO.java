package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

import java.util.List;
@Data
public class AlarmVO {
    private String taskId;
    private String nodeId;
    private List<AlarmSettingVO> alarmSettings;
    private List<AlarmRuleVO> alarmRules;
}
