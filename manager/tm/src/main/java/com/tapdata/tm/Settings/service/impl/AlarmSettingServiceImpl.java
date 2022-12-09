package com.tapdata.tm.Settings.service.impl;

import cn.hutool.extra.cglib.CglibUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.Settings.entity.AlarmSetting;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmSettingServiceImpl implements AlarmSettingService {
    private MongoTemplate mongoTemplate;

    @Override
    public void delete(List<AlarmSetting> data) {
        if (CollectionUtils.isNotEmpty(data)) {

            List<AlarmKeyEnum> collect = data.stream().map(AlarmSetting::getKey).collect(Collectors.toList());

            mongoTemplate.remove(new Query(Criteria.where("key").in(collect)), AlarmSetting.class);
        }
    }

    @Override
    public void save(List<AlarmSettingDto> alarms) {
        List<AlarmSetting> data = CglibUtil.copyList(alarms, AlarmSetting::new);

        AlarmSettingService alarmSettingService = SpringUtil.getBean(AlarmSettingService.class);
        alarmSettingService.delete(data);

        mongoTemplate.insert(data, AlarmSetting.class);
    }

    @Override
    public List<AlarmSettingDto> findAll() {
        List<AlarmSetting> alarmSettings = mongoTemplate.find(new Query(), AlarmSetting.class);

        return CglibUtil.copyList(alarmSettings, AlarmSettingDto::new);
    }



    @Override
    public void updateSystemNotify(UpdateRuleDto ruleDto) {
        Query query = new Query(Criteria.where("key").is(ruleDto.getKey()));
        Update update = new Update().set("systemNotify", ruleDto.isNotify());
        mongoTemplate.updateFirst(query, update, AlarmSetting.class);
    }

    @Override
    public AlarmSettingDto findByKey(AlarmKeyEnum keyEnum) {
        AlarmSetting one = mongoTemplate.findOne(new Query(), AlarmSetting.class);
        if (Objects.isNull(one)) {
            return null;
        }
        return CglibUtil.copy(one, AlarmSettingDto.class);
    }
}
