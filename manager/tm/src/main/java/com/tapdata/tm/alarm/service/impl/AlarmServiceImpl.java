package com.tapdata.tm.alarm.service.impl;

import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmServiceImpl implements AlarmService {

    private MongoTemplate mongoTemplate;

    @Override
    public void save(AlarmInfo info) {
        Criteria criteria = Criteria.where("taskId").is(info.getTaskId()).and("metric").is(info.getMetric())
                .and("level").is(info.getLevel());
        if (StringUtils.isNotBlank(info.getNodeId())) {
            criteria.and("nodeId").is(info.getNodeId());
        }
        Query query = new Query();
    }
}
