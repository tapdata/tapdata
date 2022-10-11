package com.tapdata.tm.alarmrule.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.extra.cglib.CglibUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.alarmrule.entity.AlarmRule;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.utils.Lists;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmRuleServiceImpl implements AlarmRuleService {

    private MongoTemplate mongoTemplate;

    @Override
    public void delete(List<AlarmRule> data) {
        if (CollectionUtils.isNotEmpty(data)) {

            List<AlarmKeyEnum> collect = data.stream().map(AlarmRule::getKey).collect(Collectors.toList());

            mongoTemplate.remove(new Query(Criteria.where("key").in(collect)), AlarmRule.class);
        }

    }

    @Override
    public void save(List<AlarmRuleDto> rules) {
        List<AlarmRule> data = CglibUtil.copyList(rules, AlarmRule::new);

        AlarmRuleService alarmRuleService = SpringUtil.getBean(AlarmRuleService.class);
        alarmRuleService.delete(data);

        mongoTemplate.insert(data, AlarmRule.class);
    }

    @Override
    public List<AlarmRuleDto> findAll() {
        List<AlarmRule> alarmRules = mongoTemplate.find(new Query(), AlarmRule.class);

        return CglibUtil.copyList(alarmRules, AlarmRuleDto::new);
    }
}
