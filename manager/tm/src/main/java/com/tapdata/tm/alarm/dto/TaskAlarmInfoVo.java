package com.tapdata.tm.alarm.dto;

import lombok.Builder;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@ToString
@Builder
public class TaskAlarmInfoVo {
    LinkedList<TaskAlarmNodeInfoVo> nodeInfos;
    List<AlarmListInfoVo> alarmList;
}
