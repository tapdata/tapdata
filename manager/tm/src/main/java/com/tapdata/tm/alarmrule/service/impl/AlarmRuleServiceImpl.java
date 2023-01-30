package com.tapdata.tm.alarmrule.service.impl;

import cn.hutool.extra.cglib.CglibUtil;
import com.tapdata.tm.alarmrule.entity.AlarmRule;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.config.security.UserDetail;
import io.tapdata.pdk.apis.functions.connector.common.ReleaseExternalFunction;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
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
    public void save(List<AlarmRuleDto> rules, UserDetail userDetail) {
        List<AlarmRule> data = CglibUtil.copyList(rules, AlarmRule::new);

        data.forEach(info -> mongoTemplate.save(info));
    }

    @Override
    public List<AlarmRuleDto> findAll(UserDetail userDetail) {
        Query query = Query.query(Criteria.where("userId").is(userDetail.getUserId()));
        List<AlarmRule> alarmRules = mongoTemplate.find(query, AlarmRule.class);

        if (CollectionUtils.isEmpty(alarmRules)) {
            query = Query.query(Criteria.where("userId").exists(false));
            alarmRules = mongoTemplate.find(query, AlarmRule.class);
            if (CollectionUtils.isNotEmpty(alarmRules)) {
                alarmRules.forEach(rule -> {
                    rule.setId(new ObjectId());
                    rule.setUserId(userDetail.getUserId());
                });
            }
        }

        return CglibUtil.copyList(alarmRules, AlarmRuleDto::new);
    }
}
