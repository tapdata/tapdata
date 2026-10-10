package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

import java.util.List;
@Data
public class AlarmVO {
    private String taskId;
    private String nodeId;
    private List<AlarmSettingVO> alarmSettings;
    private List<AlarmRuleVO> alarmRules;
    private List<String> emailReceivers;
    /** null 表示本次不修改接收对象。空列表表示自定义为空，不回落系统默认 */
    private List<AlarmReceiver> alarmReceivers;
    /** 为 true 时取消自定义接收人，改回系统默认。优先于 alarmReceivers */
    private Boolean useSystemDefaultReceivers;
}
