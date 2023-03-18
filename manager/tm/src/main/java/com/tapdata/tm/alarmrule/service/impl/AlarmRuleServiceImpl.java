package com.tapdata.tm.alarmrule.service.impl;

import cn.hutool.extra.cglib.CglibUtil;
import com.tapdata.tm.alarmrule.entity.AlarmRule;
import com.tapdata.tm.alarmrule.repository.AlarmRuleRepository;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmRuleServiceImpl extends BaseService<AlarmRuleDto, AlarmRule, ObjectId, AlarmRuleRepository> implements AlarmRuleService {

    private MongoTemplate mongoTemplate;

    public AlarmRuleServiceImpl(@NonNull AlarmRuleRepository repository) {
        super(repository, AlarmRuleDto.class, AlarmRule.class);
    }

    @Override
    public void saveAlarm(List<AlarmRuleDto> rules, UserDetail userDetail) {
        List<AlarmRule> data = CglibUtil.copyList(rules, AlarmRule::new);
        data.forEach(info -> repository.save(info,userDetail));
    }

    @Override
    public List<AlarmRuleDto> findAllAlarm(UserDetail userDetail) {
        List<AlarmRule> alarmRules = repository.findAll(userDetail);
        if (CollectionUtils.isEmpty(alarmRules)) {
           Query query = Query.query(Criteria.where("userId").exists(false));
            alarmRules = mongoTemplate.find(query, AlarmRule.class);
            if (CollectionUtils.isNotEmpty(alarmRules)) {
                alarmRules.forEach(rule -> {
                    rule.setId(null);
                });
            }
        }

        List<AlarmRule> list = alarmRules.stream().collect(
                collectingAndThen(
                        toCollection(() -> new TreeSet<>(Comparator.comparing(AlarmRule::getKey))), ArrayList::new));

        return CglibUtil.copyList(list, AlarmRuleDto::new);
    }

    @Override
    protected void beforeSave(AlarmRuleDto dto, UserDetail userDetail) {

    }
}
