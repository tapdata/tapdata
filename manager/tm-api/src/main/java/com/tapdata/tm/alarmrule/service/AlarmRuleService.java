package com.tapdata.tm.alarmrule.service;

import com.tapdata.tm.Settings.entity.AlarmSetting;
import com.tapdata.tm.Settings.repository.AlarmSettingsRepository;
import com.tapdata.tm.alarmrule.entity.AlarmRule;
import com.tapdata.tm.alarmrule.repository.AlarmRuleRepository;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.List;

public abstract class AlarmRuleService extends BaseService<AlarmRuleDto, AlarmRule, ObjectId, AlarmRuleRepository>{
    public AlarmRuleService(@NonNull AlarmRuleRepository repository) {
        super(repository, AlarmRuleDto.class, AlarmRule.class);
    }
    public abstract void saveAlarm(List<AlarmRuleDto> rules, UserDetail userDetail);
    public abstract List<AlarmRuleDto> findAllAlarm(UserDetail userDetail);
}
