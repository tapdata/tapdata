package com.tapdata.tm.alarm.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@Builder
@Data
public class TaskAlarmInfoVo {
    private List<TaskAlarmNodeInfoVo> nodeInfos;
    private List<AlarmListInfoVo> alarmList;
    private AlarmNumVo alarmNum;
}
