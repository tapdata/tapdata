package com.tapdata.tm.Settings.service;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.Settings.entity.AlarmSetting;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;

import java.util.List;

public interface AlarmSettingService {

    void delete(List<AlarmSetting> alarmSettingList);
    void save(List<AlarmSettingDto> alarms);

    List<AlarmSettingDto> findAll();

    void updateSystemNotify(UpdateRuleDto ruleDto);

    AlarmSettingDto findByKey(AlarmKeyEnum keyEnum);
}
