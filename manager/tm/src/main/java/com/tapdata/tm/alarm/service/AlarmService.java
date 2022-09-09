package com.tapdata.tm.alarm.service;

import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;

import java.util.List;

public interface AlarmService {
    void save(AlarmInfo info);

    boolean checkOpen(String taskId, String key, NotifyEnum notityType);

    List<AlarmRuleDto> findAllRule(String taskId);

    void notifyAlarm();

    void close(String id);
}
