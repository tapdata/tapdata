package com.tapdata.tm.alarmrule.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.alarmrule.dto.AlarmRuleDto;
import com.tapdata.tm.Settings.entity.Alarm;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;
import com.tapdata.tm.alarmrule.entity.AlarmRule;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.utils.Lists;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmRuleServiceImpl implements AlarmRuleService {

    private MongoTemplate mongoTemplate;

    @Override
    public void deleteAll() {
        mongoTemplate.remove(new Query(), Alarm.class);
    }

    @Override
    public void save(List<AlarmRuleDto> rules) {
        AlarmRuleService alarmRuleService = SpringUtil.getBean(AlarmRuleService.class);
        alarmRuleService.deleteAll();

        List<AlarmRule> data = Lists.newArrayList();
        BeanUtil.copyProperties(rules, data);

        mongoTemplate.insert(data, AlarmRule.class);
    }

    @Override
    public List<AlarmRuleDto> findAll() {
        List<AlarmRule> alarmRules = mongoTemplate.find(new Query(), AlarmRule.class);
        List<AlarmRuleDto> result = Lists.newArrayList();

        BeanUtil.copyProperties(alarmRules, result);

        return result;
    }
}
