package com.tapdata.tm.schedule;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmContentTemplate;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.alarm.service.impl.AlarmServiceImpl;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.commons.util.ThrowableUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import io.tapdata.common.executor.ExecutorsManager;
import io.tapdata.common.sample.request.Sample;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskAlarmScheduler {

    private TaskService taskService;
    private AlarmService alarmService;
    private MeasurementServiceV2 measurementServiceV2;
    private WorkerService workerService;
    private UserService userService;
    private SettingsService settingsService;

    private final ExecutorService executorService = ExecutorsManager.getInstance().getExecutorService();

    @Scheduled(cron = "0 0/5 * * * ? ")
    @SchedulerLock(name ="task_agent_alarm_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void taskAgentAlarm() {
        Object buildProfile = settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE);
        if (Objects.isNull(buildProfile)) {
            buildProfile = "DAAS";
        }
        boolean isCloud = buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");

        Query query = new Query(Criteria.where("status").is(TaskDto.STATUS_RUNNING)
                .and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                .and("is_deleted").is(false));
        List<TaskDto> taskDtos = taskService.findAll(query);
        if (CollectionUtils.isEmpty(taskDtos)) {
            return;
        }

        List<String> collect = taskDtos.stream().map(TaskDto::getAgentId).distinct().collect(Collectors.toList());

        Query worerQuery = new Query(Criteria.where("worker_type").is("connector")
                .and("process_id").in(collect));
        List<WorkerDto> workers = workerService.findAll(worerQuery);
        if (CollectionUtils.isEmpty(workers)) {
            return;
        }

        Object heartTime = settingsService.getValueByCategoryAndKey(CategoryEnum.WORKER, KeyEnum.WORKER_HEART_TIMEOUT);
        long heartExpire = Objects.nonNull(heartTime) ? (Long.parseLong(heartTime.toString()) + 48 ) * 1000 : 108000;

        Map<String, WorkerDto> stopEgineMap = workers.stream().filter(w -> (Objects.nonNull(w.getIsDeleted()) && w.getIsDeleted()) ||
                        (Objects.nonNull(w.getStopping()) && w.getStopping()) ||
                        w.getPingTime() < (System.currentTimeMillis() - heartExpire))
                .collect(Collectors.toMap(WorkerDto::getProcessId, Function.identity(), (e1, e2) -> e1));
        if (stopEgineMap.isEmpty()) {
            return;
        } else {
            log.error("taskAgentAlarm stopEgineMap data:{}", JSON.toJSONString(stopEgineMap));
        }

        Set<String> agentIds = stopEgineMap.keySet();

        // 云版需要修改这里
        List<TaskDto> taskList = taskDtos.stream().filter(t -> agentIds.contains(t.getAgentId())).collect(Collectors.toList());

        List<String> userIds = taskList.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
        List<UserDetail> userByIdList = userService.getUserByIdList(userIds);
        Map<String, UserDetail> userDetailMap = userByIdList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity(), (e1, e2) -> e1));

        for (TaskDto data : taskList) {
            List<Worker> workerList = workerService.findAvailableAgentBySystem(userDetailMap.get(data.getUserId()));;
            if (AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.getName().equals(data.getAccessNodeType())) {
                List<String> processIdList = data.getAccessNodeProcessIdList();
                workerList = workerList.stream().filter(w -> processIdList.contains(w.getProcessId())).collect(Collectors.toList());
            }

            String orginAgentId = data.getAgentId();
            AtomicReference<String> summary = new AtomicReference<>();
            if (CollectionUtils.isEmpty(workerList)) {
                String finalOrginAgentId = orginAgentId;
                FunctionUtils.isTureOrFalse(isCloud).trueOrFalseHandle(
                        () -> summary.set(MessageFormat.format(AlarmContentTemplate.SYSTEM_FLOW_EGINGE_DOWN_CLOUD, finalOrginAgentId, DateUtil.now())),
                        () -> summary.set(MessageFormat.format(AlarmContentTemplate.SYSTEM_FLOW_EGINGE_DOWN_NO_AGENT, finalOrginAgentId, DateUtil.now()))
                );

                AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.WARNING).component(AlarmComponentEnum.FE)
                        .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(orginAgentId).taskId(data.getId().toHexString())
                        .name(data.getName()).summary(summary.get()).metric(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_DOWN)
                        .build();
                alarmInfo.setUserId(data.getUserId());
                alarmService.save(alarmInfo);
            } else {
                if (isCloud) {
                    summary.set(MessageFormat.format(AlarmContentTemplate.SYSTEM_FLOW_EGINGE_DOWN_CLOUD, orginAgentId, DateUtil.now()));
                } else {
                    data.setAgentId(null);
                    CalculationEngineVo calculationEngineVo = workerService.scheduleTaskToEngine(data, userDetailMap.get(data.getUserId()), "task", data.getName());
                    summary.set(MessageFormat.format(AlarmContentTemplate.SYSTEM_FLOW_EGINGE_DOWN_CHANGE_AGENT, orginAgentId, workerList.size(), calculationEngineVo.getProcessId(), DateUtil.now()));
                    orginAgentId = calculationEngineVo.getProcessId();
                }

                AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.WARNING).component(AlarmComponentEnum.FE)
                        .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(orginAgentId).taskId(data.getId().toHexString())
                        .name(data.getName()).summary(summary.get()).metric(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_DOWN)
                        .build();
                alarmInfo.setUserId(data.getUserId());
                alarmService.save(alarmInfo);

                if (!isCloud) {
                    taskService.start(data, userDetailMap.get(data.getUserId()), "11");
                }
            }
        }
    }

    @Scheduled(cron = "30 0/1 * * * ? ")
    @SchedulerLock(name ="task_alarm_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void taskAlarm() {
        Query query = new Query(Criteria.where("status").is(TaskDto.STATUS_RUNNING)
                .and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                .and("is_deleted").is(false));
        List<TaskDto> all = taskService.findAll(query);

        all.forEach(task -> executorService.submit(() -> {
            String taskId = task.getId().toHexString();

            Map<String, List<AlarmRuleDto>> ruleMap = alarmService.getAlarmRuleDtos(task);
            if (!ruleMap.isEmpty()) {
                DateTime end = DateUtil.date();
                DateTime start = DateUtil.offsetMinute(end, -1);

                Query measureQuery = new Query(Criteria.where("grnty").is("minute")
                        .and("tags.type").in(Lists.newArrayList("task", "node"))
                        .and("tags.taskId").is(taskId)
                        .and("tags.taskRecordId").is(task.getTaskRecordId())
                        .and("date").gt(start).lt(end));
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

            String flag = alarmRuleDto.getEqualsFlag() == -1 ? "小于" : "大于";

            AtomicInteger delay = new AtomicInteger(0);
            long count = samples.stream().filter(ss -> {
                int current = Math.abs((int) ss.getVs().getOrDefault(avgName, 0));
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
                }
                return b;
            }).count();

            List<AlarmInfo> alarmInfos = alarmService.find(taskId, nodeId, alarmKeyEnum);

            AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId)
                    .name(task.getName()).metric(alarmKeyEnum)
                    .nodeId(nodeId).node(nodeName)
                    .build();
            alarmInfo.setUserId(task.getUserId());
            if (count >= alarmRuleDto.getPoint()) {
                String summary;
                Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus())).findFirst();
                Number current = delay.get();
                if (first.isPresent()) {
                    AlarmInfo data = first.get();
                    alarmInfo.setId(data.getId());
                    alarmInfo.setStatus(AlarmStatusEnum.RECOVER);
                    alarmInfo.setLastOccurrenceTime(null);

                    long continued = DateUtil.between(data.getFirstOccurrenceTime(), DateUtil.date(), DateUnit.MINUTE);
                    summary = MessageFormat.format(template[3], nodeName, alarmRuleDto.getMs(), continued, current, DateUtil.now(), flag);
                } else {
                    alarmInfo.setStatus(AlarmStatusEnum.ING);
                    summary = MessageFormat.format(template[2], nodeName, alarmRuleDto.getMs(), current, DateUtil.now(), flag);
                }
                alarmInfo.setLevel(Level.WARNING);
                alarmInfo.setSummary(summary);
                Map<String, Object> param = Maps.newHashMap();
                param.put("interval", alarmRuleDto.getMs());
                param.put("current", current);
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
    }

    private void taskIncrementDelayAlarm(TaskDto task, String taskId, List<Sample> taskSamples, Map<AlarmKeyEnum, AlarmRuleDto> collect) {
        // check task start cdc
        if (Objects.isNull(task.getCurrentEventTimestamp())) {
            return;
        }

        if (collect.containsKey(AlarmKeyEnum.TASK_INCREMENT_DELAY)) {
            AlarmRuleDto alarmRuleDto = collect.get(AlarmKeyEnum.TASK_INCREMENT_DELAY);

            String flag = alarmRuleDto.getEqualsFlag() == -1 ? "小于" : "大于";

            AtomicInteger delay = new AtomicInteger(0);
            long count = taskSamples.stream().filter(ss -> {
                int replicateLag = Math.abs((int) ss.getVs().getOrDefault("replicateLag", 0));

                boolean b;
                if (alarmRuleDto.getEqualsFlag() == -1) {
                    b = replicateLag <= alarmRuleDto.getMs();
                } else if (alarmRuleDto.getEqualsFlag() == 1) {
                    b = replicateLag >= alarmRuleDto.getMs();
                } else {
                    b = false;
                }
                if (b) {
                    delay.set(replicateLag);
                }
                return b;
            }).count();

            List<AlarmInfo> alarmInfos = alarmService.find(taskId, null, AlarmKeyEnum.TASK_INCREMENT_DELAY);

            AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId)
                    .name(task.getName()).metric(AlarmKeyEnum.TASK_INCREMENT_DELAY)
                    .build();
            alarmInfo.setUserId(task.getUserId());
            if (count >= alarmRuleDto.getPoint()) {
                String summary;
                Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus())).findFirst();
                alarmInfo.setStatus(AlarmStatusEnum.ING);
                if (first.isPresent()) {
                    AlarmInfo data = first.get();
                    alarmInfo.setId(data.getId());

                    long continued = DateUtil.between(data.getFirstOccurrenceTime(), DateUtil.date(), DateUnit.MINUTE);
                    summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_DELAY_ALWAYS, alarmRuleDto.getMs(), continued, delay, DateUtil.now(), flag);
                } else {
                    summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_DELAY_START, alarmRuleDto.getMs(), delay, DateUtil.now(), flag);
                    Map<String, Object> param = Maps.newHashMap();
                    param.put("time", delay);
                    alarmInfo.setParam(param);
                }
                alarmInfo.setLevel(Level.WARNING);
                alarmInfo.setSummary(summary);
                alarmService.save(alarmInfo);
            } else {
                Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus()) || AlarmStatusEnum.RECOVER.equals(info.getStatus())).findFirst();
                if (first.isPresent()) {
                    //Number current = taskSamples.get(0).getVs().get("replicateLag");
                    String summary = MessageFormat.format(AlarmContentTemplate.TASK_INCREMENT_DELAY_RECOVER, delay, DateUtil.now());

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
    }
}
