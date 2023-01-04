package com.tapdata.tm.base.aop;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.alarm.constant.AlarmContentTemplate;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.worker.dto.WorkerDto;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.List;

@Aspect
@Component
@Setter(onMethod_ = {@Autowired})
public class WorkerAOP {

    private AlarmService alarmService;

    @AfterReturning("execution(* com.tapdata.tm.worker.service.WorkerService.health(..))")
    public void workerHealth(JoinPoint joinPoint) {
        WorkerDto worker = (WorkerDto) joinPoint.getArgs()[0];
        String processId = worker.getProcessId();

        Criteria criteria = Criteria.where("status").is(AlarmStatusEnum.ING)
                .and("metric").is(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_DOWN.name())
                .and("agentId").is(processId);
        Query alarmQuery = Query.query(criteria);
        List<AlarmInfo> alarmInfos = alarmService.query(alarmQuery);
        if (CollectionUtils.isEmpty(alarmInfos)) {
            return;
        }

        alarmInfos.forEach(alarmInfo -> {
            alarmInfo.setStatus(AlarmStatusEnum.RECOVER);
            String summary = MessageFormat.format(AlarmContentTemplate.SYSTEM_FLOW_EGINGE_RECOVER, processId, DateUtil.now());
            alarmInfo.setSummary(summary);
            alarmInfo.setRecoveryTime(DateUtil.date());
            alarmService.save(alarmInfo);
        });
    }
}
