package com.tapdata.tm.base.aop;

import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;


@Aspect
@Component
@Setter(onMethod_ = {@Autowired})
public class MeasureAOP {

    private TaskService taskService;
    private AlarmService alarmService;
    private final Map<String, Map<String, AtomicInteger>> obsMap = Maps.newConcurrentMap();


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
        String alarmDate = DateUtil.now();
        if (Objects.isNull(taskDto.getSnapshotDoneAt()) && Objects.nonNull(snapshotStartAt) && Objects.nonNull(snapshotDoneAt)) {

            Long diff = (Long)snapshotDoneAt - (Long)snapshotStartAt;

            Map<String, Object> param = Maps.newHashMap();
            param.put("costTime", diff);
            param.put("snapDoneDate", now);
            param.put("alarmDate", alarmDate);
            param.put("taskName", taskDto.getName());

            AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.NORMAL).component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(taskDto.getAgentId()).taskId(taskId)
                    .name(taskDto.getName()).summary("TASK_FULL_COMPLETE").metric(AlarmKeyEnum.TASK_FULL_COMPLETE)
                    .param(param)
                    .build();

            alarmInfo.setUserId(taskDto.getUserId());
            alarmService.save(alarmInfo);
        }

        Number currentEventTimestamp = vs.get("currentEventTimestamp");
        if (Objects.isNull(taskDto.getCurrentEventTimestamp()) && Objects.nonNull(currentEventTimestamp)) {
            Map<String, Object> param = Maps.newHashMap();
            param.put("cdcTime", now);
            param.put("alarmDate", alarmDate);
            param.put("taskName", taskDto.getName());

            AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.NORMAL).component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(taskDto.getAgentId()).taskId(taskId)
                    .name(taskDto.getName()).summary("TASK_INCREMENT_START").metric(AlarmKeyEnum.TASK_INCREMENT_START)
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

        AtomicInteger taskReplicateLagCount = new AtomicInteger();

        Map<String, AtomicInteger> infoMap = obsMap.get(taskId);
        if (Objects.nonNull(infoMap) && Objects.nonNull(infoMap.get(key))) {
            taskReplicateLagCount.set(infoMap.get(key).intValue());
        } else {
            infoMap = Maps.newHashMap();
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
        } else {
            taskReplicateLagCount.set(0);
        }
        infoMap.put(key, taskReplicateLagCount);
        obsMap.put(taskId, infoMap);

        List<AlarmInfo> alarmInfos = alarmService.find(taskId, null, AlarmKeyEnum.TASK_INCREMENT_DELAY);

        AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE)
                .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId)
                .name(task.getName()).metric(AlarmKeyEnum.TASK_INCREMENT_DELAY)
                .build();
        alarmInfo.setUserId(task.getUserId());
        String alarmDate = DateUtil.now();
        Map<String, Object> param = Maps.newHashMap();
        if (taskReplicateLagCount.get() >= alarmRuleDto.getPoint()) {
            String summary;
            Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus())).findFirst();
            alarmInfo.setStatus(AlarmStatusEnum.ING);
            if (first.isPresent()) {
                AlarmInfo data = first.get();
                alarmInfo.setId(data.getId());

                long continued = DateUtil.between(data.getFirstOccurrenceTime(), DateUtil.date(), DateUnit.MINUTE);
                summary = "TASK_INCREMENT_DELAY_ALWAYS";
                param.put("flag", flag);
                param.put("alarmDate", alarmDate);
                param.put("taskName", task.getName());
                param.put("threshold", alarmRuleDto.getMs());
                param.put("currentValue", replicateLag);
                param.put("continueTime", continued);
                alarmInfo.setParam(param);
            } else {
                summary = "TASK_INCREMENT_DELAY_START";
                param.put("flag", flag);
                param.put("alarmDate", alarmDate);
                param.put("taskName", task.getName());
                param.put("threshold", alarmRuleDto.getMs());
                param.put("currentValue", replicateLag);
                alarmInfo.setParam(param);
            }
            alarmInfo.setLevel(Level.WARNING);
            alarmInfo.setSummary(summary);
            alarmService.save(alarmInfo);
        } else {
            Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus()) || AlarmStatusEnum.RECOVER.equals(info.getStatus())).findFirst();
            if (first.isPresent()) {
                String summary = "TASK_INCREMENT_DELAY_RECOVER";

                param.put("alarmDate", alarmDate);
                param.put("taskName", task.getName());
                param.put("currentValue", replicateLag);
                alarmInfo.setParam(param);

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

        AtomicInteger count = new AtomicInteger();
        Map<String, AtomicInteger> infoMap = obsMap.get(taskId);
        if (Objects.nonNull(infoMap) && Objects.nonNull(infoMap.get(key))) {
            count.set(infoMap.get(key).intValue());
        } else {
            infoMap = Maps.newHashMap();
        }

        String flag = alarmRuleDto.getEqualsFlag() == -1 ? "LESS" : "GREATER";
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
        } else {
            count.set(0);
        }
        infoMap.put(key, count);
        obsMap.put(taskId, infoMap);

        List<AlarmInfo> alarmInfos = alarmService.find(taskId, nodeId, alarmKeyEnum);

        AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE)
                .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId)
                .name(task.getName()).metric(alarmKeyEnum)
                .nodeId(nodeId).node(nodeName)
                .build();
        alarmInfo.setUserId(task.getUserId());
        Map<String, Object> param = Maps.newHashMap();
        String alarmDate = DateUtil.now();
        param.put("alarmDate", alarmDate);
        param.put("taskName", task.getName());
        param.put("nodeName", nodeName);
        param.put("flag", flag);
        param.put("threshold", alarmRuleDto.getMs());
        param.put("currentValue", delay.get());
        // 【Warning】任务[MySQL-2-Oracle] 的源节点[CUSTOMER]平均处理耗时超过阈值500ms，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
        if (count.get() >= alarmRuleDto.getPoint()) {
            String summary;
            Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus())).findFirst();
            alarmInfo.setStatus(AlarmStatusEnum.ING);
            if (first.isPresent()) {
                AlarmInfo data = first.get();
                alarmInfo.setId(data.getId());

                long continued = DateUtil.between(data.getFirstOccurrenceTime(), DateUtil.date(), DateUnit.MINUTE);
                param.put("continueTime", continued);

                summary = template[3];
            } else {
                summary = template[2];
            }
            alarmInfo.setParam(param);
            alarmInfo.setLevel(Level.WARNING);
            alarmInfo.setSummary(summary);

            alarmService.save(alarmInfo);
        } else {
            Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus()) || AlarmStatusEnum.RECOVER.equals(info.getStatus())).findFirst();
            if (first.isPresent()) {
                //Number current = samples.get(0).getVs().get(avgName);
                String summary = template[4];

                alarmInfo.setId(first.get().getId());
                alarmInfo.setLevel(Level.RECOVERY);
                alarmInfo.setSummary(summary);
                alarmInfo.setRecoveryTime(DateUtil.date());
                alarmInfo.setFirstOccurrenceTime(null);
                alarmInfo.setParam(param);
                alarmService.save(alarmInfo);
            }
        }
    }

    private String[] getTemplate(AlarmKeyEnum alarmKeyEnum, Boolean source) {
        String[] result = null;
        if (alarmKeyEnum == AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME) {
            if (source) {
                result = new String[]{AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME.name(), "snapshotSourceReadTimeCostAvg",
                        "DATANODE_AVERAGE_HANDLE_CONSUME_START",
                        "DATANODE_AVERAGE_HANDLE_CONSUME_ALWAYS",
                        "DATANODE_AVERAGE_HANDLE_CONSUME_RECOVER"};
            } else {
                result = new String[]{AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME.name(), "targetWriteTimeCostAvg",
                        "TARGET_AVERAGE_HANDLE_CONSUME_START",
                        "TARGET_AVERAGE_HANDLE_CONSUME_ALWAYS",
                        "TARGET_AVERAGE_HANDLE_CONSUME_RECOVER"};
            }
        } else if (alarmKeyEnum == AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME) {
            result = new String[]{AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME.name(), "timeCostAvg",
                    "PROCESSNODE_AVERAGE_HANDLE_CONSUME_START",
                    "PROCESSNODE_AVERAGE_HANDLE_CONSUME_ALWAYS",
                    "PROCESSNODE_AVERAGE_HANDLE_CONSUME_RECOVER"};
        }

        return result;
    }

    public void removeObsInfoByTaskId(String taskId) {
        obsMap.remove(taskId);
    }
}
