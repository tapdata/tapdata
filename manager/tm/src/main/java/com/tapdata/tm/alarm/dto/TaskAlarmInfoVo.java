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
    List<TaskAlarmNodeInfoVo> nodeInfos;
    List<AlarmListInfoVo> alarmList;
}
