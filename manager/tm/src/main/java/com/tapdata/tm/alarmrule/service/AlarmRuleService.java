package com.tapdata.tm.alarmrule.service;

import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.alarmrule.entity.AlarmRule;
import com.tapdata.tm.config.security.UserDetail;

import java.util.List;

public interface AlarmRuleService {
    void save(List<AlarmRuleDto> rules, UserDetail userDetail);
    List<AlarmRuleDto> findAll(UserDetail userDetail);
}
