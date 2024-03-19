package com.tapdata.tm.Settings.service;

import com.tapdata.tm.Settings.entity.AlarmSetting;
import com.tapdata.tm.Settings.repository.AlarmSettingsRepository;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.entity.RoleMappingEntity;
import com.tapdata.tm.roleMapping.repository.RoleMappingRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.List;

public abstract class AlarmSettingService extends BaseService<AlarmSettingDto, AlarmSetting, ObjectId, AlarmSettingsRepository> {
    public AlarmSettingService(@NonNull AlarmSettingsRepository repository) {
        super(repository, AlarmSettingDto.class, AlarmSetting.class);
    }
    public abstract void saveAlarmSetting(List<AlarmSettingDto> alarms, UserDetail userDetail);

    public abstract List<AlarmSettingDto> findAllAlarmSetting(UserDetail userDetail);

    public abstract void updateSystemNotify(UpdateRuleDto ruleDto, UserDetail userDetail);

    public abstract AlarmSettingDto findByKey(AlarmKeyEnum keyEnum, UserDetail userDetail);
}
