package com.tapdata.tm.schedule.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.schedule.constant.ScheduleJobEnum;
import com.tapdata.tm.schedule.entity.ScheduleJobInfo;
import com.tapdata.tm.schedule.service.ScheduleJobService;
import com.tapdata.tm.utils.Lists;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

/**
 * @author jiuyetx
 * @date 2022/9/8
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class ScheduleJobServiceImpl implements ScheduleJobService {

    private MongoTemplate mongoTemplate;

    @Override
    public void save(List<ScheduleJobInfo> jobs) {
        List<ScheduleJobInfo> insertList = Lists.newArrayList();
        for (ScheduleJobInfo job : jobs) {
            Query query = new Query(Criteria.where("groupName").is(job.getGroupName()).and("jobName").is(job.getJobName()));
            ScheduleJobInfo d = mongoTemplate.findOne(query, ScheduleJobInfo.class);

            if (Objects.nonNull(d)) {
                BeanUtil.copyProperties(job, d);
                mongoTemplate.save(d);
            } else {
                insertList.add(job);
            }
        }

        if (CollectionUtils.isNotEmpty(insertList)) {
            mongoTemplate.insert(insertList);
        }
    }

    @Override
    public List<ScheduleJobInfo> listByCode(String code) {
        Criteria criteria = Criteria.where("code").is(code).and("status").is(ScheduleJobEnum.ING);
        return mongoTemplate.find(new Query(criteria), ScheduleJobInfo.class);
    }
}
