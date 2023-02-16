package com.tapdata.tm.Settings.service.impl;

import cn.hutool.extra.cglib.CglibUtil;
import com.tapdata.tm.Settings.entity.AlarmSetting;
import com.tapdata.tm.Settings.repository.AlarmSettingsRepository;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;
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
public class AlarmSettingServiceImpl  extends BaseService<AlarmSettingDto, AlarmSetting, ObjectId, AlarmSettingsRepository> implements AlarmSettingService{
    private MongoTemplate mongoTemplate;

    private UserService  userService;

    public AlarmSettingServiceImpl(@NonNull AlarmSettingsRepository repository) {
        super(repository, AlarmSettingDto.class, AlarmSetting.class);
    }


    @Override
    public void saveAlarmSetting(List<AlarmSettingDto> alarms, UserDetail userDetail) {
        List<AlarmSetting> data = CglibUtil.copyList(alarms, AlarmSetting::new);
        if (CollectionUtils.isNotEmpty(data)) {
            data.forEach(info -> repository.save(info,userDetail));
        }
    }

    @Override
    public List<AlarmSettingDto> findAllAlarmSetting(UserDetail userDetail) {
       // Query query = Query.query(Criteria.where("userId").is(userDetail.getUserId()));
        List<AlarmSetting> alarmSettings = repository.findAll(userDetail);
        if (CollectionUtils.isEmpty(alarmSettings)) {
            Query  query = Query.query(Criteria.where("userId").exists(false));
            alarmSettings = mongoTemplate.find(query, AlarmSetting.class);
            if (CollectionUtils.isNotEmpty(alarmSettings)) {
                alarmSettings.forEach(sett -> {
                    sett.setId(new ObjectId());
                    sett.setUserId(userDetail.getUserId());
                });
            }
        }
        List<AlarmSetting> list = alarmSettings.stream().collect(
                collectingAndThen(
                        toCollection(() -> new TreeSet<>(Comparator.comparing(AlarmSetting::getKey))), ArrayList::new));

        return CglibUtil.copyList(list, AlarmSettingDto::new);
    }

    @Override
    protected void beforeSave(AlarmSettingDto dto, UserDetail userDetail) {

    }

    @Override
    public void updateSystemNotify(UpdateRuleDto ruleDto, UserDetail userDetail) {
        Query query = new Query(Criteria.where("key").is(ruleDto.getKey()));
        Update update = new Update().set("systemNotify", ruleDto.isNotify());
        repository.updateFirst(query, update, userDetail);
    }

    @Override
    public AlarmSettingDto findByKey(AlarmKeyEnum keyEnum, UserDetail userDetail) {
        Query query = Query.query(Criteria.where("key").is(keyEnum.name()));
        AlarmSettingDto alarmSetting = findOne(query, userDetail);
        if (Objects.isNull(alarmSetting)) {
            query = Query.query(Criteria.where("userId").exists(false));
            alarmSetting = mongoTemplate.findOne(query, AlarmSettingDto.class);
        }

        if (Objects.isNull(alarmSetting)) {
            return null;
        }
        return alarmSetting;
    }
}
