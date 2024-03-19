package com.tapdata.tm.alarmrule.service.impl;

import cn.hutool.extra.cglib.CglibUtil;
import com.tapdata.tm.alarmrule.entity.AlarmRule;
import com.tapdata.tm.alarmrule.repository.AlarmRuleRepository;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
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
public class AlarmRuleServiceImpl extends  AlarmRuleService {
    public AlarmRuleServiceImpl(@NonNull AlarmRuleRepository repository) {
        super(repository);
    }
    @Override
    public void saveAlarm(List<AlarmRuleDto> rules, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<AlarmRuleDto> findAllAlarm(UserDetail userDetail) {
        return new ArrayList<>();
    }

    @Override
    protected void beforeSave(AlarmRuleDto dto, UserDetail userDetail) {

    }
}
