package com.tapdata.tm.alarm.scheduler;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmContentTemplate;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import io.tapdata.common.executor.ExecutorsManager;
import io.tapdata.common.sample.request.Sample;
import lombok.Setter;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Setter(onMethod_ = {@Autowired})
public class TaskAlarmScheduler {

    private TaskService taskService;
    private AlarmService alarmService;
    private MeasurementServiceV2 measurementServiceV2;

    private final ExecutorService executorService = ExecutorsManager.getInstance().getExecutorService();

    @Scheduled(initialDelay = 5000, fixedRate = 30000)
    @SchedulerLock(name ="task_alarm_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void taskAlarm() {

        Query query = new Query(Criteria.where("status").is(TaskDto.STATUS_RUNNING)
                .and("is_deleted").ne(true));
        List<TaskDto> all = taskService.findAll(query);

        all.forEach(task -> executorService.submit(() -> {
            String taskId = task.getId().toHexString();

            Map<String, List<AlarmRuleDto>> ruleMap = alarmService.getAlarmRuleDtos(task);
            if (!ruleMap.isEmpty()) {
                DateTime end = DateUtil.date();
                DateTime start = DateUtil.offsetMinute(end, -1);

                Query measureQuery = new Query(Criteria.where("grnty").is("minute")
                        .and("tags.type").in(Lists.newArrayList("task", "node"))
                        .and("tags.taskId").is(taskId).and("date").gte(start).lt(end));
                List<MeasurementEntity> measurementEntities = measurementServiceV2.find(measureQuery);
                if (CollectionUtils.isNotEmpty(measurementEntities)) {
                    List<Sample> taskSamples = Lists.newArrayList();
                    Map<String, List<Sample>> databaseMap = Maps.newHashMap();
                    Map<String, List<Sample>> processMap = Maps.newHashMap();
                    for (MeasurementEntity measurement : measurementEntities) {
                        Map<String, String> tags = measurement.getTags();
                        String type = tags.get("type");
                        String nodeType = tags.get("nodeType");
                        if ("task".equals(type)) {
                            taskSamples = measurement.getSamples();
                        } else if ("database".equals(nodeType)){
                            databaseMap.put(tags.get("nodeId"), measurement.getSamples());
                        } else if ("processor".equals(nodeType)){
                            processMap.put(tags.get("nodeId"), measurement.getSamples());
                        }
                    }

                    List<Node> sources = task.getDag().getSources();
                    List<Node> targets = task.getDag().getTargets();
                    List<Node> allNodes = task.getDag().getNodes();
                    allNodes.removeAll(sources);
                    allNodes.removeAll(targets);

                    for (Map.Entry<String, List<AlarmRuleDto>> entry : ruleMap.entrySet()) {
                        String key = entry.getKey();
                        List<AlarmRuleDto> rules = entry.getValue();

                        Map<AlarmKeyEnum, AlarmRuleDto> collect = Maps.newHashMap();
                        if (CollectionUtils.isNotEmpty(rules)) {
                            collect = rules.stream().collect(Collectors.toMap(AlarmRuleDto::getKey, Function.identity(), (e1, e2) -> e1));
                        }

                        Optional<Node> sourceNode = sources.stream().filter(node -> node.getId().equals(key)).findFirst();
                        Optional<Node> targetNode = targets.stream().filter(node -> node.getId().equals(key)).findFirst();
                        Optional<Node> processNode = allNodes.stream().filter(node -> node.getId().equals(key)).findFirst();


                        if (taskId.equals(key)) {
                            taskIncrementDelayAlarm(task, taskId, taskSamples, collect);
                        } else if (sourceNode.isPresent()) {
                            supplmentDelayAvg(task, taskId, taskSamples, collect, key,sourceNode.get().getName(), getTemplate(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME, true));
                        } else if (targetNode.isPresent()) {
                            supplmentDelayAvg(task, taskId, databaseMap.get(key), collect, key,targetNode.get().getName(), getTemplate(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME, false));
                        } else if (processNode.isPresent()) {
                            supplmentDelayAvg(task, taskId, processMap.get(key), collect, key,processNode.get().getName(), getTemplate(AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME, null));
                        }
                    }

                }

            }

        }));
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

    private void supplmentDelayAvg(TaskDto task, String taskId, List<Sample> samples, Map<AlarmKeyEnum, AlarmRuleDto> collect, String nodeId, String nodeName, String[] template) {
        AlarmKeyEnum alarmKeyEnum = AlarmKeyEnum.valueOf(template[0]);
        String avgName = template[1];
        if (collect.containsKey(alarmKeyEnum)) {
            AlarmRuleDto alarmRuleDto = collect.get(alarmKeyEnum);
            long count = samples.stream().filter(ss -> {
                if (alarmRuleDto.getEqualsFlag() == -1) {
                    return (int) ss.getVs().getOrDefault(avgName, 0) <= alarmRuleDto.getMs();
                } else if (alarmRuleDto.getEqualsFlag() == 1) {
                    return (int) ss.getVs().getOrDefault(avgName, 0) >= alarmRuleDto.getMs();
                } else {
                    return false;
                }
            }).count();

            List<AlarmInfo> alarmInfos = alarmService.find(taskId, nodeId, alarmKeyEnum);

            AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId)
                    .name(task.getName()).metric(alarmKeyEnum)
                    .nodeId(nodeId).node(nodeName)
                    .build();
            if (count >= alarmRuleDto.getPoint()) {
                String summary;
                Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus())).findFirst();
                Number current = samples.get(0).getVs().get(avgName);
                if (first.isPresent()) {
                    AlarmInfo data = first.get();
                    alarmInfo.setId(data.getId());
                    alarmInfo.setStatus(AlarmStatusEnum.RECOVER);

                    long continued = DateUtil.between(data.getFirstOccurrenceTime(), DateUtil.date(), DateUnit.MINUTE);
                    summary = MessageFormat.format(template[3], nodeName, alarmRuleDto.getMs(), continued, current, DateUtil.now());
                } else {
                    alarmInfo.setStatus(AlarmStatusEnum.ING);
                    summary = MessageFormat.format(template[2], nodeName, alarmRuleDto.getMs(), current, DateUtil.now());
                }
                alarmInfo.setLevel(Level.WARNING);
                alarmInfo.setSummary(summary);
                alarmService.save(alarmInfo);
            } else {
                Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus()) || AlarmStatusEnum.RECOVER.equals(info.getStatus())).findFirst();
                if (first.isPresent()) {
                    Number current = samples.get(0).getVs().get(avgName);
                    String summary = MessageFormat.format(template[4], nodeName, current, DateUtil.now());

                    alarmInfo.setId(first.get().getId());
                    alarmInfo.setLevel(Level.RECOVERY);
                    alarmInfo.setSummary(summary);
                    alarmInfo.setRecoveryTime(DateUtil.date());
                    alarmService.save(alarmInfo);
                }
            }
        }
    }

    private void taskIncrementDelayAlarm(TaskDto task, String taskId, List<Sample> taskSamples, Map<AlarmKeyEnum, AlarmRuleDto> collect) {
        if (collect.containsKey(AlarmKeyEnum.TASK_INCREMENT_DELAY)) {
            AlarmRuleDto alarmRuleDto = collect.get(AlarmKeyEnum.TASK_INCREMENT_DELAY);
            long count = taskSamples.stream().filter(ss -> {
                if (alarmRuleDto.getEqualsFlag() == -1) {
                    return (int) ss.getVs().getOrDefault("replicateLag", 0) <= alarmRuleDto.getMs();
                } else if (alarmRuleDto.getEqualsFlag() == 1) {
                    return (int) ss.getVs().getOrDefault("replicateLag", 0) >= alarmRuleDto.getMs();
                } else {
                    return false;
                }
            }).count();

            List<AlarmInfo> alarmInfos = alarmService.find(taskId, null, AlarmKeyEnum.TASK_INCREMENT_DELAY);

            AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId)
                    .name(task.getName()).metric(AlarmKeyEnum.TASK_INCREMENT_DELAY)
                    .build();
            if (count >= alarmRuleDto.getPoint()) {
                String summary;
                Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus())).findFirst();
                Number current = taskSamples.get(0).getVs().get("replicateLag");
                if (first.isPresent()) {
                    AlarmInfo data = first.get();
                    alarmInfo.setId(data.getId());
                    alarmInfo.setStatus(AlarmStatusEnum.RECOVER);

                    long continued = DateUtil.between(data.getFirstOccurrenceTime(), DateUtil.date(), DateUnit.MINUTE);
                    summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_DELAY_ALWAYS, alarmRuleDto.getMs(), continued, current, DateUtil.now());
                } else {
                    alarmInfo.setStatus(AlarmStatusEnum.ING);
                    summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_DELAY_START, alarmRuleDto.getMs(), current, DateUtil.now());
                }
                alarmInfo.setLevel(Level.WARNING);
                alarmInfo.setSummary(summary);
                alarmService.save(alarmInfo);
            } else {
                Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus()) || AlarmStatusEnum.RECOVER.equals(info.getStatus())).findFirst();
                if (first.isPresent()) {
                    Number current = taskSamples.get(0).getVs().get("replicateLag");
                    String summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_DELAY_RECOVER, current, DateUtil.now());

                    alarmInfo.setId(first.get().getId());
                    alarmInfo.setLevel(Level.RECOVERY);
                    alarmInfo.setSummary(summary);
                    alarmInfo.setRecoveryTime(DateUtil.date());
                    alarmService.save(alarmInfo);
                }
            }
        }
    }
}
