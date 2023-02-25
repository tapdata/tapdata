package com.tapdata.tm.alarmrule.service;

import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.alarmrule.entity.AlarmRule;

import java.util.List;

public interface AlarmRuleService {
    void delete(List<AlarmRule> data);
    void save(List<AlarmRuleDto> rules);

    List<AlarmRuleDto> findAll();
}
