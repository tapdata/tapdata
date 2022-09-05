package com.tapdata.tm.alarmrule.service;

import com.tapdata.tm.alarmrule.dto.AlarmRuleDto;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;

import java.util.List;

public interface AlarmRuleService {
    void deleteAll();
    void save(List<AlarmRuleDto> rules);

    List<AlarmRuleDto> findAll();
}
