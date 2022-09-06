package com.tapdata.tm.Settings.service;

import com.tapdata.tm.Settings.dto.AlarmDto;
import com.tapdata.tm.Settings.entity.Alarm;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;

import java.util.List;

public interface AlarmService {

    void delete(List<Alarm> alarmList);
    void save(List<AlarmDto> alarms);

    List<AlarmDto> findAll();

    void updateSystemNotify(UpdateRuleDto ruleDto);
}
