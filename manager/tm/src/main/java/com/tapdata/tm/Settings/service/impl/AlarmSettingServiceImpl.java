package com.tapdata.tm.Settings.service.impl;

import cn.hutool.extra.cglib.CglibUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.Settings.entity.AlarmSetting;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
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
import java.util.stream.Collectors;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmSettingServiceImpl implements AlarmSettingService {
    private MongoTemplate mongoTemplate;

    @Override
    public void save(List<AlarmSettingDto> alarms, UserDetail userDetail) {
        List<AlarmSetting> data = CglibUtil.copyList(alarms, AlarmSetting::new);

        if (CollectionUtils.isNotEmpty(data)) {
            data.forEach(info -> mongoTemplate.save(info));
        }
    }

    @Override
    public List<AlarmSettingDto> findAll(UserDetail userDetail) {
        Query query = Query.query(Criteria.where("userId").is(userDetail.getUserId()));
        List<AlarmSetting> alarmSettings = mongoTemplate.find(query, AlarmSetting.class);
        if (CollectionUtils.isEmpty(alarmSettings)) {
            query = Query.query(Criteria.where("userId").exists(false));
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
    public void updateSystemNotify(UpdateRuleDto ruleDto, UserDetail userDetail) {
        Query query = new Query(Criteria.where("key").is(ruleDto.getKey()).and("userId").is(userDetail.getUserId()));
        Update update = new Update().set("systemNotify", ruleDto.isNotify());
        mongoTemplate.updateFirst(query, update, AlarmSetting.class);
    }

    @Override
    public AlarmSettingDto findByKey(AlarmKeyEnum keyEnum, String userId) {
        Query query = Query.query(Criteria.where("userId").is(userId));
        AlarmSetting one = mongoTemplate.findOne(query, AlarmSetting.class);
        if (Objects.isNull(one)) {
            query = Query.query(Criteria.where("userId").exists(false));
            one = mongoTemplate.findOne(query, AlarmSetting.class);
        }

        if (Objects.isNull(one)) {
            return null;
        }
        return CglibUtil.copy(one, AlarmSettingDto.class);
    }
}
