package com.tapdata.tm.Settings.service;

import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;

import java.util.List;

public interface AlarmSettingService {
    void saveAlarmSetting(List<AlarmSettingDto> alarms, UserDetail userDetail);

    List<AlarmSettingDto> findAllAlarmSetting(UserDetail userDetail);

    void updateSystemNotify(UpdateRuleDto ruleDto, UserDetail userDetail);

    AlarmSettingDto findByKey(AlarmKeyEnum keyEnum, UserDetail userDetail);
}
