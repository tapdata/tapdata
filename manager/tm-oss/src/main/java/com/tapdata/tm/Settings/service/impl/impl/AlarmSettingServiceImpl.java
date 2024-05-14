package com.tapdata.tm.Settings.service.impl.impl;

import cn.hutool.extra.cglib.CglibUtil;
import com.tapdata.tm.Settings.entity.AlarmSetting;
import com.tapdata.tm.Settings.repository.AlarmSettingsRepository;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmSettingServiceImpl extends AlarmSettingService{
    public AlarmSettingServiceImpl(@NonNull AlarmSettingsRepository repository) {
        super(repository);
    }
    @Override
    public void saveAlarmSetting(List<AlarmSettingDto> alarms, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<AlarmSettingDto> findAllAlarmSetting(UserDetail userDetail) {
        return new ArrayList<>();
    }

    @Override
    public void updateSystemNotify(UpdateRuleDto ruleDto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public AlarmSettingDto findByKey(AlarmKeyEnum keyEnum, UserDetail userDetail) {
        return null;
    }

    @Override
    protected void beforeSave(AlarmSettingDto dto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
