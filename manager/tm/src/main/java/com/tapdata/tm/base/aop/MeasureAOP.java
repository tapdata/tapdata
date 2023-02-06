package com.tapdata.tm.base.aop;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmContentTemplate;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.common.sample.request.SampleRequest;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;


@Aspect
@Component
@Setter(onMethod_ = {@Autowired})
public class MeasureAOP {

    private TaskService taskService;
    private AlarmService alarmService;

    @AfterReturning("execution(* com.tapdata.tm.monitor.service.MeasurementServiceV2.addAgentMeasurement(..))")
    public void addAgentMeasurement(JoinPoint joinPoint) {
        List<SampleRequest> samples = (List<SampleRequest>)joinPoint.getArgs()[0];

        samples.forEach(sampleRequest -> {
            String type = sampleRequest.getTags().get("type");
            if ("task".equals(type)) {
                String taskId = sampleRequest.getTags().get("taskId");
                TaskDto taskDto = taskService.findByTaskId(MongoUtils.toObjectId(taskId), "name", "snapshotStartAt", "snapshotDoneAt", "currentEventTimestamp");

                String now = DateUtil.now();
                Map<String, Number> vs = sampleRequest.getSample().getVs();
                Number snapshotStartAt = vs.get("snapshotStartAt");
                Number snapshotDoneAt = vs.get("snapshotDoneAt");
                if (Objects.isNull(taskDto.getSnapshotDoneAt()) && Objects.nonNull(snapshotStartAt) && Objects.nonNull(snapshotDoneAt)) {

                    Long diff = (Long)snapshotDoneAt - (Long)snapshotStartAt;
                    String summary = MessageFormat.format(AlarmContentTemplate.TASK_FULL_COMPLETE, diff, DateUtil.date((Long) snapshotDoneAt).toString(), now);

                    Map<String, Object> param = Maps.newHashMap();
                    param.put("fullTime", now);

                    AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.NORMAL).component(AlarmComponentEnum.FE)
                            .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(taskDto.getAgentId()).taskId(taskId)
                            .name(taskDto.getName()).summary(summary).metric(AlarmKeyEnum.TASK_FULL_COMPLETE)
                            .param(param)
                            .build();

                    alarmInfo.setUserId(taskDto.getUserId());
                    alarmService.save(alarmInfo);
                }

                Number currentEventTimestamp = vs.get("currentEventTimestamp");
                if (Objects.isNull(taskDto.getCurrentEventTimestamp()) && Objects.nonNull(currentEventTimestamp)) {
                    String summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_START, DateUtil.date((Long) currentEventTimestamp).toString(), now);
                    Map<String, Object> param = Maps.newHashMap();
                    param.put("cdcTime", now);

                    AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.NORMAL).component(AlarmComponentEnum.FE)
                            .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(taskDto.getAgentId()).taskId(taskId)
                            .name(taskDto.getName()).summary(summary).metric(AlarmKeyEnum.TASK_INCREMENT_START)
                            .param(param)
                            .build();
                    alarmInfo.setUserId(taskDto.getUserId());
                    alarmService.save(alarmInfo);
                }

                Update update = new Update();

                if (Objects.nonNull(snapshotDoneAt) && !snapshotDoneAt.equals(taskDto.getSnapshotDoneAt())) {
                    update.set("snapshotDoneAt", snapshotDoneAt);
                }
                if (Objects.nonNull(currentEventTimestamp) && !currentEventTimestamp.equals(taskDto.getCurrentEventTimestamp())) {
                    update.set("currentEventTimestamp", currentEventTimestamp);
                }

                if (update.getUpdateObject().size() > 0) {
                    taskService.update(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(taskId))), update);
                }
            }
        });
    }
}
