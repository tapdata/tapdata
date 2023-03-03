package com.tapdata.tm.base.aop;

import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.sun.org.apache.bcel.internal.generic.IFGE;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmContentTemplate;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;


@Aspect
@Component
@Setter(onMethod_ = {@Autowired})
public class MeasureAOP {

    private TaskService taskService;
    private AlarmService alarmService;
    private final Map<String, AtomicInteger> obsMap = Maps.newConcurrentMap();

    @AfterReturning("execution(* com.tapdata.tm.monitor.service.MeasurementServiceV2.addAgentMeasurement(..))")
    public void addAgentMeasurement(JoinPoint joinPoint) {
        List<SampleRequest> samples = (List<SampleRequest>) joinPoint.getArgs()[0];

        samples.forEach(sampleRequest -> {
            String type = sampleRequest.getTags().get("type");
            String taskId = sampleRequest.getTags().get("taskId");
            Map<String, Number> vs = sampleRequest.getSample().getVs();

            if (StringUtils.isNotBlank(taskId)) {
                TaskDto taskDto = taskService.findByTaskId(MongoUtils.toObjectId(taskId));
                // task alarm
                Map<String, List<AlarmRuleDto>> ruleMap = alarmService.getAlarmRuleDtos(taskDto);
                if (!ruleMap.isEmpty()) {
                    if ("task".equals(type)) {
                        Optional.ofNullable(ruleMap.get(taskId)).ifPresent(rules -> {
                            Map<AlarmKeyEnum, AlarmRuleDto> collect = rules.stream().collect(Collectors.toMap(AlarmRuleDto::getKey, Function.identity(), (e1, e2) -> e1));
                            if (!collect.isEmpty()) {
                                taskIncrementDelayAlarm(taskDto, taskId, vs.get("replicateLag"), collect.get(AlarmKeyEnum.TASK_INCREMENT_DELAY));
                            }
                        });
                        setTaskSnapshotDate(vs, taskId, taskDto);
                    } else if ("node".equals(type)) {
                        String nodeId = sampleRequest.getTags().get("nodeId");
                        Optional.ofNullable(ruleMap.get(nodeId)).ifPresent(rules -> {
                            Map<AlarmKeyEnum, AlarmRuleDto> collect = rules.stream().collect(Collectors.toMap(AlarmRuleDto::getKey, Function.identity(), (e1, e2) -> e1));
                            if (!collect.isEmpty()) {
                                DAG dag = taskDto.getDag();
                                Optional<Node> sourceNode = dag.getSources().stream().filter(node -> node.getId().equals(nodeId)).findFirst();
                                Optional<Node> targetNode = dag.getTargets().stream().filter(node -> node.getId().equals(nodeId)).findFirst();
                                String[] template;
                                String nodeName = dag.getNode(nodeId).getName();
                                if (sourceNode.isPresent()) {
                                    template = getTemplate(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME, true);
                                } else if (targetNode.isPresent()) {
                                    template = getTemplate(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME, false);
                                } else {
                                    template = getTemplate(AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME, null);
                                }
                                supplmentDelayAvg(taskDto, taskId, vs.get(template[1]), collect.get(AlarmKeyEnum.valueOf(template[0])), nodeId, nodeName, template);
                            }
                        });

                    }
                }
            }
        });
    }

    private void setTaskSnapshotDate(Map<String, Number> vs, String taskId, TaskDto taskDto) {
        String now = DateUtil.now();
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

    private void taskIncrementDelayAlarm(TaskDto task, String taskId, Number replicateLag, AlarmRuleDto alarmRuleDto) {
        // check task start cdc
        if (Objects.isNull(task.getCurrentEventTimestamp()) || Objects.isNull(replicateLag)) {
            return;
        }

        String key = taskId + "-" + "replicateLag";

        AtomicInteger taskReplicateLagCount = obsMap.get(key);
        if (Objects.isNull(taskReplicateLagCount)) {
            taskReplicateLagCount = new AtomicInteger();
        }

        String flag = alarmRuleDto.getEqualsFlag() == -1 ? "小于" : "大于";

        boolean b;
        if (alarmRuleDto.getEqualsFlag() == -1) {
            b = replicateLag.intValue() <= alarmRuleDto.getMs();
        } else if (alarmRuleDto.getEqualsFlag() == 1) {
            b = replicateLag.intValue() >= alarmRuleDto.getMs();
        } else {
            b = false;
        }
        if (b) {
            taskReplicateLagCount.incrementAndGet();
        }
        obsMap.put(key, taskReplicateLagCount);

        List<AlarmInfo> alarmInfos = alarmService.find(taskId, null, AlarmKeyEnum.TASK_INCREMENT_DELAY);

        AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE)
                .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId)
                .name(task.getName()).metric(AlarmKeyEnum.TASK_INCREMENT_DELAY)
                .build();
        alarmInfo.setUserId(task.getUserId());
        if (taskReplicateLagCount.get() >= alarmRuleDto.getPoint()) {
            String summary;
            Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus())).findFirst();
            alarmInfo.setStatus(AlarmStatusEnum.ING);
            if (first.isPresent()) {
                AlarmInfo data = first.get();
                alarmInfo.setId(data.getId());

                long continued = DateUtil.between(data.getFirstOccurrenceTime(), DateUtil.date(), DateUnit.MINUTE);
                summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_DELAY_ALWAYS, alarmRuleDto.getMs(), continued, replicateLag, DateUtil.now(), flag);
            } else {
                summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_DELAY_START, alarmRuleDto.getMs(), replicateLag, DateUtil.now(), flag);
                Map<String, Object> param = Maps.newHashMap();
                param.put("time", replicateLag);
                alarmInfo.setParam(param);
            }
            alarmInfo.setLevel(Level.WARNING);
            alarmInfo.setSummary(summary);
            alarmService.save(alarmInfo);
        } else {
            Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus()) || AlarmStatusEnum.RECOVER.equals(info.getStatus())).findFirst();
            if (first.isPresent()) {
                String summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_DELAY_RECOVER, replicateLag, DateUtil.now());

                alarmInfo.setId(first.get().getId());
                alarmInfo.setLevel(Level.RECOVERY);
                alarmInfo.setStatus(AlarmStatusEnum.RECOVER);
                alarmInfo.setSummary(summary);
                alarmInfo.setRecoveryTime(DateUtil.date());
                alarmInfo.setLastOccurrenceTime(null);
                alarmService.save(alarmInfo);
            }
        }
    }

    private void supplmentDelayAvg(TaskDto task, String taskId, Number number, AlarmRuleDto alarmRuleDto, String nodeId, String nodeName, String[] template) {
        if (Objects.isNull(number) || Objects.isNull(alarmRuleDto)) {
            return;
        }

        AlarmKeyEnum alarmKeyEnum = AlarmKeyEnum.valueOf(template[0]);
        String avgName = template[1];

        String key = nodeId + "-" + avgName;

        AtomicInteger count = obsMap.get(key);
        if (Objects.isNull(obsMap.get(key))) {
            count = new AtomicInteger();
        }


        String flag = alarmRuleDto.getEqualsFlag() == -1 ? "小于" : "大于";
        AtomicInteger delay = new AtomicInteger(0);

        int current = Math.abs(number.intValue());
        boolean b;
        if (alarmRuleDto.getEqualsFlag() == -1) {
            b = current <= alarmRuleDto.getMs();
        } else if (alarmRuleDto.getEqualsFlag() == 1) {
            b = current >= alarmRuleDto.getMs();
        } else {
            b = false;
        }
        if (b) {
            delay.set(current);
            count.incrementAndGet();
        }
        obsMap.put(key, count);

        List<AlarmInfo> alarmInfos = alarmService.find(taskId, nodeId, alarmKeyEnum);

        AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE)
                .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId)
                .name(task.getName()).metric(alarmKeyEnum)
                .nodeId(nodeId).node(nodeName)
                .build();
        alarmInfo.setUserId(task.getUserId());
        if (count.get() >= alarmRuleDto.getPoint()) {
            String summary;
            Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus())).findFirst();
            if (first.isPresent()) {
                AlarmInfo data = first.get();
                alarmInfo.setId(data.getId());
                alarmInfo.setStatus(AlarmStatusEnum.RECOVER);
                alarmInfo.setLastOccurrenceTime(null);

                long continued = DateUtil.between(data.getFirstOccurrenceTime(), DateUtil.date(), DateUnit.MINUTE);
                summary = MessageFormat.format(template[3], nodeName, alarmRuleDto.getMs(), continued, delay.get(), DateUtil.now(), flag);
            } else {
                alarmInfo.setStatus(AlarmStatusEnum.ING);
                summary = MessageFormat.format(template[2], nodeName, alarmRuleDto.getMs(), delay.get(), DateUtil.now(), flag);
            }
            alarmInfo.setLevel(Level.WARNING);
            alarmInfo.setSummary(summary);
            Map<String, Object> param = Maps.newHashMap();
            param.put("interval", alarmRuleDto.getMs());
            param.put("current", delay.get());
            alarmInfo.setParam(param);
            alarmService.save(alarmInfo);
        } else {
            Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus()) || AlarmStatusEnum.RECOVER.equals(info.getStatus())).findFirst();
            if (first.isPresent()) {
                //Number current = samples.get(0).getVs().get(avgName);
                String summary = MessageFormat.format(template[4], nodeName, delay, DateUtil.now(), flag);

                alarmInfo.setId(first.get().getId());
                alarmInfo.setLevel(Level.RECOVERY);
                alarmInfo.setSummary(summary);
                alarmInfo.setRecoveryTime(DateUtil.date());
                alarmService.save(alarmInfo);
            }
        }
    }

    private String[] getTemplate(AlarmKeyEnum alarmKeyEnum, Boolean source) {
        String[] result = null;
        if (alarmKeyEnum == AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME) {
            if (source) {
                result = new String[]{AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME.name(), "snapshotSourceReadTimeCostAvg",
                        AlarmContentTemplate.DATANODE_AVERAGE_HANDLE_CONSUME_START,
                        AlarmContentTemplate.DATANODE_AVERAGE_HANDLE_CONSUME_ALWAYS,
                        AlarmContentTemplate.DATANODE_AVERAGE_HANDLE_CONSUME_RECOVER};
            } else {
                result = new String[]{AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME.name(), "targetWriteTimeCostAvg",
                        AlarmContentTemplate.TARGET_AVERAGE_HANDLE_CONSUME_START,
                        AlarmContentTemplate.TARGET_AVERAGE_HANDLE_CONSUME_ALWAYS,
                        AlarmContentTemplate.TARGET_AVERAGE_HANDLE_CONSUME_RECOVER};
            }
        } else if (alarmKeyEnum == AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME) {
            result = new String[]{AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME.name(), "timeCostAvg",
                    AlarmContentTemplate.PROCESSNODE_AVERAGE_HANDLE_CONSUME_START,
                    AlarmContentTemplate.PROCESSNODE_AVERAGE_HANDLE_CONSUME_ALWAYS,
                    AlarmContentTemplate.PROCESSNODE_AVERAGE_HANDLE_CONSUME_RECOVER};
        }

        return result;
    }
}
