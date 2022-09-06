package com.tapdata.tm.Settings.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.Settings.dto.AlarmDto;
import com.tapdata.tm.Settings.entity.Alarm;
import com.tapdata.tm.Settings.service.AlarmService;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;
import com.tapdata.tm.alarmrule.entity.AlarmRule;
import com.tapdata.tm.utils.Lists;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmServiceImpl implements AlarmService {
    private MongoTemplate mongoTemplate;

    @Override
    public void delete(List<Alarm> data) {
        if (CollectionUtils.isNotEmpty(data)) {

            List<String> collect = data.stream().map(Alarm::getKey).collect(Collectors.toList());

            mongoTemplate.remove(new Query(Criteria.where("key").in(collect)), Alarm.class);
        }
    }

    @Override
    public void save(List<AlarmDto> alarms) {
        List<Alarm> data = Lists.newArrayList();
        BeanUtil.copyProperties(alarms, data);

        AlarmService alarmService = SpringUtil.getBean(AlarmService.class);
        alarmService.delete(data);

        mongoTemplate.insert(data, Alarm.class);
    }

    @Override
    public List<AlarmDto> findAll() {
        List<Alarm> alarms = mongoTemplate.find(new Query(), Alarm.class);

        List<AlarmDto> result = Lists.newArrayList();

        BeanUtil.copyProperties(alarms, result);

        return result;
    }

    @Override
    public void updateSystemNotify(UpdateRuleDto ruleDto) {
        Query query = new Query(Criteria.where("key").is(ruleDto.getKey()));
        Update update = new Update().set("systemNotify", ruleDto.isNotify());
        mongoTemplate.updateFirst(query, update, Alarm.class);
    }
}
