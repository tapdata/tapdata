package com.tapdata.tm.alarmrule.service;

import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.config.security.UserDetail;

import java.util.List;

public interface AlarmRuleService {
    void saveAlarm(List<AlarmRuleDto> rules, UserDetail userDetail);
    List<AlarmRuleDto> findAllAlarm(UserDetail userDetail);
}
