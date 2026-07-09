package com.tapdata.tm.base.aop;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.commons.alarm.AlarmComponentEnum;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.TimeUtil;
import io.tapdata.common.sample.request.SampleRequest;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;


@Aspect
@Component
public class MeasureAOP {
    public static final String GREATER = "GREATER";
    public static final String LESS = "LESS";

    private final TaskService taskService;
    private final AlarmService alarmService;
    private final UserService userService;
    private final AlarmSettingService alarmSettingService;
    private final DataSourceService dataSourceService;
//    private final InspectService inspectService;
    private final Map<String, Map<String, AtomicInteger>> obsMap = Maps.newConcurrentMap();

    private static final String FIELD_TAGS_TYPE = MetricCons.Tags.F_TYPE;
    private static final String FIELD_TAGS_TASK_ID = MetricCons.Tags.F_TASK_ID;
    private static final String FIELD_TAGS_NODE_ID = MetricCons.Tags.F_NODE_ID;
    private static final String FIELD_SS_VS_REPLICATE_LAG = MetricCons.SS.VS.F_REPLICATE_LAG;
    private static final String FIELD_SS_VS_CURR_EVENT_TS = MetricCons.SS.VS.F_CURR_EVENT_TS;
    private static final String FIELD_SS_VS_SNAPSHOT_START_AT = MetricCons.SS.VS.F_SNAPSHOT_START_AT;
    private static final String FIELD_SS_VS_SNAPSHOT_DONE_AT = MetricCons.SS.VS.F_SNAPSHOT_DONE_AT;
    private static final String FIELD_SS_VS_SOURCE_INCREMENTAL_MONITOR_START_AT = MetricCons.SS.VS.F_SOURCE_INCREMENTAL_MONITOR_START_AT;
    private static final String FIELD_SS_VS_LAST_CAPTURED_INCREMENTAL_EVENT_AT = MetricCons.SS.VS.F_LAST_CAPTURED_INCREMENTAL_EVENT_AT;
    private static final String FIELD_SS_VS_LAST_ENQUEUED_INCREMENTAL_EVENT_AT = MetricCons.SS.VS.F_LAST_ENQUEUED_INCREMENTAL_EVENT_AT;
    private static final String FIELD_SS_VS_PENDING_INCREMENTAL_EVENT = MetricCons.SS.VS.F_PENDING_INCREMENTAL_EVENT;
    private static final long SOURCE_NO_INCREMENTAL_EVENT_THRESHOLD_MS = TimeUnit.MINUTES.toMillis(1);

    public MeasureAOP(TaskService taskService, AlarmService alarmService, UserService userService,
                      AlarmSettingService alarmSettingService, DataSourceService dataSourceService) {
        this.taskService = taskService;
        this.alarmService = alarmService;
        this.userService = userService;
        this.alarmSettingService = alarmSettingService;
        this.dataSourceService = dataSourceService;
    }


    @AfterReturning("execution(* com.tapdata.tm.monitor.service.MeasurementServiceV2.addAgentMeasurement(..))")
    public void addAgentMeasurement(JoinPoint joinPoint) {
        List<SampleRequest> samples = (List<SampleRequest>) joinPoint.getArgs()[0];
        Set<String> taskIds = samples.stream().filter(sampleRequest -> sampleRequest.getTags().containsKey(FIELD_TAGS_TASK_ID)).map(sampleRequest -> sampleRequest.getTags().get(FIELD_TAGS_TASK_ID)).collect(Collectors.toSet());
        Map<String, TaskDto> taskDtoMap = new HashMap<>();
        Map<String, UserDetail> userDetailMap = new HashMap<>();
        Map<String, List<AlarmSettingDto>> alarmSettingMap = new HashMap<>();
        Map<String, Boolean> heartbeatEnabledMap = new HashMap<>();
        if(CollectionUtils.isNotEmpty(taskIds)){
            taskIds.forEach(taskId -> {
                TaskDto taskDto = taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name", FIELD_SS_VS_CURR_EVENT_TS,"alarmSettings","alarmRules", FIELD_SS_VS_SNAPSHOT_DONE_AT,"status","desc", "nodeCurrentEventTimestamp");
                taskDtoMap.put(taskId, taskDto);
            });
            taskDtoMap.values().forEach(taskDto -> {
                if (Objects.isNull(taskDto) || StringUtils.isBlank(taskDto.getUserId())) {
                    return;
                }
                UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(taskDto.getUserId()));
                if (Objects.isNull(userDetail)) {
                    return;
                }
                List<AlarmSettingDto> alarmSettingDtos = alarmSettingService.findAllAlarmSetting(userDetail);
                userDetailMap.put(taskDto.getUserId(), userDetail);
                alarmSettingMap.put(userDetail.getUserId(), alarmSettingDtos);
            });
        }
        samples.forEach(sampleRequest -> {
            String type = sampleRequest.getTags().get(FIELD_TAGS_TYPE);
            String taskId = sampleRequest.getTags().get(FIELD_TAGS_TASK_ID);
            Map<String, Number> vs = sampleRequest.getSample().getVs();

            if (StringUtils.isNotBlank(taskId)) {
                TaskDto taskDto = taskDtoMap.get(taskId);
                if (Objects.isNull(taskDto) || StringUtils.isBlank(taskDto.getUserId())) {
                    return;
                }
                // task alarm
                Map<String, List<AlarmRuleDto>> ruleMap = alarmService.getAlarmRuleDtos(taskDto);
                UserDetail userDetail = userDetailMap.get(taskDto.getUserId());
                if (Objects.isNull(userDetail)) {
                    return;
                }
                if (MetricCons.SampleType.TASK.check(type)) {
                    if (null != ruleMap && !ruleMap.isEmpty()) {
                        boolean checkOpen = alarmService.checkOpen(taskDto, null, AlarmKeyEnum.TASK_INCREMENT_DELAY, null, alarmSettingMap.get(userDetail.getUserId()));
                        if (checkOpen) {
                            Optional.ofNullable(ruleMap.get(taskId)).ifPresent(rules -> {
                                Map<AlarmKeyEnum, AlarmRuleDto> collect = rules.stream().collect(Collectors.toMap(AlarmRuleDto::getKey, Function.identity(), (e1, e2) -> e1));
                                if (!collect.isEmpty()) {
                                    taskIncrementDelayAlarm(taskDto, taskId, vs.get(FIELD_SS_VS_REPLICATE_LAG), collect.get(AlarmKeyEnum.TASK_INCREMENT_DELAY));
                                }
                            });
                        }
                    }
                    setTaskSnapshotDate(vs, taskId, taskDto, userDetail);
                } else if (MetricCons.SampleType.NODE.check(type)) {
                    String nodeId = sampleRequest.getTags().get(FIELD_TAGS_NODE_ID);
                    DAG dag = taskDto.getDag();
                    if (StringUtils.isBlank(nodeId) || Objects.isNull(dag)) {
                        return;
                    }
                    Node<?> currentNode = dag.getNode(nodeId);
                    if (Objects.isNull(currentNode)) {
                        return;
                    }
                    Optional<Node> sourceNode = CollectionUtils.emptyIfNull(dag.getSources()).stream().filter(node -> node.getId().equals(nodeId)).findFirst();
                    Optional<Node> targetNode = CollectionUtils.emptyIfNull(dag.getTargets()).stream().filter(node -> node.getId().equals(nodeId)).findFirst();
                    String nodeName = currentNode.getName();
                    if (sourceNode.isPresent()) {
                        sourceNoIncrementalEventAlarm(taskDto, taskId, nodeId, nodeName, vs, sourceNode.get(),
                                heartbeatEnabledMap, alarmSettingMap.get(userDetail.getUserId()));

                        Number currentEventTimestamp = vs.get(FIELD_SS_VS_CURR_EVENT_TS);
                        if (Objects.nonNull(currentEventTimestamp) && currentEventTimestamp.longValue() > 0) {
                            Map<String, Long> nodeCurrentEventTimestampMap = taskDto.getNodeCurrentEventTimestamp();
                            if (nodeCurrentEventTimestampMap == null || !currentEventTimestamp.equals(nodeCurrentEventTimestampMap.get(nodeId))) {
                                Update update = new Update();
                                update.set("nodeCurrentEventTimestamp." + nodeId, currentEventTimestamp.longValue());
                                taskService.update(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(taskId))), update);

                                if (nodeCurrentEventTimestampMap == null) {
                                    nodeCurrentEventTimestampMap = new HashMap<>();
                                    taskDto.setNodeCurrentEventTimestamp(nodeCurrentEventTimestampMap);
                                }
                                nodeCurrentEventTimestampMap.put(nodeId, currentEventTimestamp.longValue());
                            }
                        }
                    }
                    if (null != ruleMap && !ruleMap.isEmpty()) {
                        Optional.ofNullable(ruleMap.get(nodeId)).ifPresent(rules -> {
                            Map<AlarmKeyEnum, AlarmRuleDto> collect = rules.stream().collect(Collectors.toMap(AlarmRuleDto::getKey, Function.identity(), (e1, e2) -> e1));
                            if (!collect.isEmpty()) {
                                String[] template;
                                boolean checkOpen;
                                if (sourceNode.isPresent()) {
                                    checkOpen = alarmService.checkOpen(taskDto, nodeId, AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME, null, alarmSettingMap.get(userDetail.getUserId()));
                                    template = getTemplate(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME, true);
                                } else if (targetNode.isPresent()) {
                                    checkOpen = alarmService.checkOpen(taskDto, nodeId, AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME, null, alarmSettingMap.get(userDetail.getUserId()));
                                    template = getTemplate(AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME, false);
                                } else {
                                    checkOpen = alarmService.checkOpen(taskDto, nodeId, AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME, null, alarmSettingMap.get(userDetail.getUserId()));
                                    template = getTemplate(AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME, null);
                                }

                                if (checkOpen) {
                                    supplmentDelayAvg(taskDto, taskId, vs.get(template[1]), collect.get(AlarmKeyEnum.valueOf(template[0])), nodeId, nodeName, template);
                                }
                            }
                        });
                    }
                }
            }
        });
    }

    protected void setTaskSnapshotDate(Map<String, Number> vs, String taskId, TaskDto taskDto, UserDetail userDetail) {
        String now = DateUtil.now();
        Number snapshotStartAt = vs.get(FIELD_SS_VS_SNAPSHOT_START_AT);
        Number snapshotDoneAt = vs.get(FIELD_SS_VS_SNAPSHOT_DONE_AT);
        String alarmDate = DateUtil.now();
        boolean checkFullOpen = alarmService.checkOpen(taskDto, null, AlarmKeyEnum.TASK_FULL_COMPLETE, null, userDetail);
        if (checkFullOpen && Objects.isNull(taskDto.getSnapshotDoneAt()) && Objects.nonNull(snapshotStartAt) && Objects.nonNull(snapshotDoneAt) && TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())) {
            Long diff = (Long) snapshotDoneAt - (Long) snapshotStartAt;

            Map<String, Object> param = Maps.newHashMap();
            param.put("costTime", diff);
            param.put("snapDoneDate", now);
            param.put("alarmDate", alarmDate);
            param.put("taskName", taskDto.getName());

            AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.NORMAL).component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(taskDto.getAgentId()).taskId(taskId).desc(taskDto.getDesc())
                    .name(taskDto.getName()).summary("TASK_FULL_COMPLETE").metric(AlarmKeyEnum.TASK_FULL_COMPLETE)
                    .param(param)
                    .build();

            alarmInfo.setUserId(taskDto.getUserId());
            alarmService.save(alarmInfo);

            // excute inspect task
//            CommonUtils.ignoreAnyError(() -> {
//                InspectDto inspectDto = inspectService.createCheckByTask(taskDto, userDetail);
//                if (inspectDto != null) {
//                    inspectService.executeInspect(Where.where("id", inspectDto.getId().toHexString()), inspectDto, userDetail);
//                }
//            }, "excute inspect task");

        }

        Number currentEventTimestamp = vs.get(FIELD_SS_VS_CURR_EVENT_TS);
        boolean checkCdcOpen = alarmService.checkOpen(taskDto, null, AlarmKeyEnum.TASK_INCREMENT_START, null, userDetail);
        if (checkCdcOpen && Objects.isNull(taskDto.getCurrentEventTimestamp()) && Objects.nonNull(currentEventTimestamp) && currentEventTimestamp.longValue() > 0L  && TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())) {
            Map<String, Object> param = Maps.newHashMap();
            param.put("cdcTime", DateUtil.format(DateUtil.date(currentEventTimestamp.longValue()), "yyyy-MM-dd HH:mm:ss"));
            param.put("alarmDate", alarmDate);
            param.put("taskName", taskDto.getName());

            AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.NORMAL).component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(taskDto.getAgentId()).taskId(taskId).desc(taskDto.getDesc())
                    .name(taskDto.getName()).summary("TASK_INCREMENT_START").metric(AlarmKeyEnum.TASK_INCREMENT_START)
                    .param(param)
                    .build();
            alarmInfo.setUserId(taskDto.getUserId());
            alarmService.save(alarmInfo);
        }

        Update update = new Update();

        if (Objects.nonNull(snapshotDoneAt) && !snapshotDoneAt.equals(taskDto.getSnapshotDoneAt())) {
            update.set(FIELD_SS_VS_SNAPSHOT_DONE_AT, snapshotDoneAt);
        }
        if (Objects.nonNull(currentEventTimestamp) && !currentEventTimestamp.equals(taskDto.getCurrentEventTimestamp())
                && !currentEventTimestamp.equals(0)) {
            update.set(FIELD_SS_VS_CURR_EVENT_TS, currentEventTimestamp);
        }

        if (update.getUpdateObject().size() > 0) {
            taskService.update(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(taskId))), update);
        }
    }

    protected void sourceNoIncrementalEventAlarm(TaskDto task, String taskId, String nodeId, String nodeName,
                                                 Map<String, Number> vs, Node<?> sourceNode,
                                                 Map<String, Boolean> heartbeatEnabledMap,
                                                 List<AlarmSettingDto> settingDtos) {
        if (null == task || !TaskDto.STATUS_RUNNING.equals(task.getStatus()) || null == vs || vs.isEmpty()) {
            return;
        }
        if (!isSourceHeartbeatEnabled(sourceNode, heartbeatEnabledMap)) {
            return;
        }
        if (!alarmService.checkOpen(task, nodeId, AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT, null, settingDtos)) {
            return;
        }
        Number monitorStartAt = vs.get(FIELD_SS_VS_SOURCE_INCREMENTAL_MONITOR_START_AT);
        Number lastCapturedIncrementalEventAt = vs.get(FIELD_SS_VS_LAST_CAPTURED_INCREMENTAL_EVENT_AT);
        Number lastEnqueuedIncrementalEventAt = vs.get(FIELD_SS_VS_LAST_ENQUEUED_INCREMENTAL_EVENT_AT);
        Number pendingIncrementalEvent = vs.get(FIELD_SS_VS_PENDING_INCREMENTAL_EVENT);
        Long monitorStart = toPositiveLong(monitorStartAt);
        Long lastCapturedAt = toPositiveLong(lastCapturedIncrementalEventAt);
        if (Objects.isNull(monitorStart) && Objects.isNull(lastCapturedAt)) {
            return;
        }
        long now = System.currentTimeMillis();
        Long lastEnqueuedAt = toPositiveLong(lastEnqueuedIncrementalEventAt);
        long baselineAt = Objects.isNull(monitorStart) ? lastCapturedAt :
                Objects.isNull(lastCapturedAt) ? monitorStart : Math.max(monitorStart, lastCapturedAt);
        boolean hasPendingIncrementalEvent = Objects.nonNull(pendingIncrementalEvent) && pendingIncrementalEvent.intValue() > 0;
        boolean downstreamBlocked = hasPendingIncrementalEvent && Objects.nonNull(lastCapturedAt) && lastCapturedAt > 0L
                && (Objects.isNull(lastEnqueuedAt) || lastCapturedAt > lastEnqueuedAt);

        List<AlarmInfo> alarmInfos = Optional.ofNullable(alarmService.find(taskId, nodeId, AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT))
                .orElse(Collections.emptyList());
        Optional<AlarmInfo> existing = alarmInfos.stream()
                .filter(info -> AlarmStatusEnum.ING.equals(info.getStatus()))
                .findFirst();

        if (downstreamBlocked || now - baselineAt < SOURCE_NO_INCREMENTAL_EVENT_THRESHOLD_MS) {
            if (existing.isPresent()) {
                saveSourceNoIncrementalEventRecoverAlarm(task, taskId, nodeId, nodeName, existing.get(), lastCapturedAt);
            }
            return;
        }

        if (existing.isPresent()) {
            return;
        }
        saveSourceNoIncrementalEventStartAlarm(task, taskId, nodeId, nodeName, lastCapturedAt, baselineAt, now);
    }

    private Long toPositiveLong(Number number) {
        if (Objects.isNull(number) || number.longValue() <= 0L) {
            return null;
        }
        return number.longValue();
    }

    private boolean isSourceHeartbeatEnabled(Node<?> sourceNode, Map<String, Boolean> heartbeatEnabledMap) {
        if (!(sourceNode instanceof DataParentNode<?> dataParentNode) || StringUtils.isBlank(dataParentNode.getConnectionId())) {
            return false;
        }
        String connectionId = dataParentNode.getConnectionId();
        return heartbeatEnabledMap.computeIfAbsent(connectionId, key -> {
            if (null == dataSourceService) {
                return false;
            }
            DataSourceConnectionDto connectionDto = dataSourceService.findById(MongoUtils.toObjectId(key), "heartbeatEnable");
            return null != connectionDto && Boolean.TRUE.equals(connectionDto.getHeartbeatEnable());
        });
    }

    private void saveSourceNoIncrementalEventStartAlarm(TaskDto task, String taskId, String nodeId, String nodeName,
                                                        Long lastCapturedAt, long baselineAt, long now) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("taskName", task.getName());
        param.put("nodeName", nodeName);
        param.put("alarmDate", DateUtil.now());
        long idleMillis = now - baselineAt;
        long totalSeconds = TimeUnit.MILLISECONDS.toSeconds(idleMillis);
        param.put("idleDuration", TimeUtil.secondsToHhmmss(totalSeconds));
        param.put("lastCaptureTime", DateUtil.format(DateUtil.date(baselineAt), "yyyy-MM-dd HH:mm:ss"));
        if (Objects.nonNull(lastCapturedAt) && lastCapturedAt > 0L) {
            param.put("latestCaptureTime", DateUtil.format(DateUtil.date(lastCapturedAt), "yyyy-MM-dd HH:mm:ss"));
        }
        AlarmInfo alarmInfo = AlarmInfo.builder()
                .status(AlarmStatusEnum.ING)
                .level(Level.WARNING)
                .component(AlarmComponentEnum.FE)
                .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM)
                .agentId(task.getAgentId())
                .taskId(taskId)
                .desc(task.getDesc())
                .nodeId(nodeId)
                .name(task.getName())
                .summary("TASK_SOURCE_NO_INCREMENTAL_EVENT_START")
                .metric(AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT)
                .param(param)
                .build();
        alarmInfo.setUserId(task.getUserId());
        alarmService.save(alarmInfo);
    }

    private void saveSourceNoIncrementalEventRecoverAlarm(TaskDto task, String taskId, String nodeId, String nodeName,
                                                          AlarmInfo existing, Long lastCapturedAt) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("taskName", task.getName());
        param.put("nodeName", nodeName);
        param.put("alarmDate", DateUtil.now());
        if (Objects.nonNull(lastCapturedAt) && lastCapturedAt > 0L) {
            param.put("latestCaptureTime", DateUtil.format(DateUtil.date(lastCapturedAt), "yyyy-MM-dd HH:mm:ss"));
        }
        AlarmInfo alarmInfo = AlarmInfo.builder()
                .status(AlarmStatusEnum.RECOVER)
                .level(Level.RECOVERY)
                .component(AlarmComponentEnum.FE)
                .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM)
                .agentId(task.getAgentId())
                .taskId(taskId)
                .desc(task.getDesc())
                .nodeId(nodeId)
                .name(task.getName())
                .summary("TASK_SOURCE_NO_INCREMENTAL_EVENT_RECOVER")
                .metric(AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT)
                .param(param)
                .build();
        alarmInfo.setId(existing.getId());
        alarmInfo.setUserId(task.getUserId());
        alarmService.save(alarmInfo);
    }

    protected void taskIncrementDelayAlarm(TaskDto task, String taskId, Number replicateLag, AlarmRuleDto alarmRuleDto) {
        // check task start cdc
        if (Objects.isNull(task.getCurrentEventTimestamp()) || Objects.isNull(replicateLag)) {
            return;
        }

        String key = taskId + "-" + "replicateLag";

        AtomicInteger taskReplicateLagCount = new AtomicInteger();

        Map<String, AtomicInteger> infoMap = obsMap.get(taskId);
        if (Objects.nonNull(infoMap) && Objects.nonNull(infoMap.get(key))) {
            taskReplicateLagCount.set(infoMap.get(key).intValue());
        } else if (Objects.isNull(infoMap)){
            infoMap = Maps.newHashMap();
        }

        String flag = alarmRuleDto.getEqualsFlag() == -1 ? LESS : GREATER;

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
                .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId).desc(task.getDesc())
                .name(task.getName()).metric(AlarmKeyEnum.TASK_INCREMENT_DELAY)
                .build();
        alarmInfo.setUserId(task.getUserId());
        String alarmDate = DateUtil.now();
        Map<String, Object> param = Maps.newHashMap();
        if (taskReplicateLagCount.get() >= alarmRuleDto.getPoint()) {
            String summary;
            Optional<AlarmInfo> first = alarmInfos.stream().filter(info -> AlarmStatusEnum.ING.equals(info.getStatus())).findFirst();
            param.put("flag", flag);
            param.put("alarmDate", alarmDate);
            param.put("taskName", task.getName());
            param.put("threshold", alarmRuleDto.getMs());
            param.put("currentValue", replicateLag);

            boolean needInspect = false;
            if (first.isPresent()) {
                AlarmInfo data = first.get();
                alarmInfo.setId(data.getId());

                long continued = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - data.getFirstOccurrenceTime().getTime());
                if (continued > 0) {
                    summary = "TASK_INCREMENT_DELAY_ALWAYS";
                    param.put("continueTime", continued);
                } else {
                    summary = "TASK_INCREMENT_DELAY_START";
                }

                if (data.getTally() == 1 || data.getTally() == 2) {
                    needInspect = true;
                }
            } else {
                summary = "TASK_INCREMENT_DELAY_START";
                needInspect = true;
            }

            alarmInfo.setStatus(AlarmStatusEnum.ING);
            alarmInfo.setFirstOccurrenceTime(null);
            alarmInfo.setParam(param);
            alarmInfo.setLevel(Level.WARNING);
            alarmInfo.setSummary(summary);
            taskService.updateTaskIncrementDelayAlarm(MongoUtils.toObjectId(taskId), replicateLag.longValue(), (long) alarmRuleDto.getMs());
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
                taskService.updateTaskIncrementDelayAlarm(MongoUtils.toObjectId(taskId), null, null);
                alarmService.save(alarmInfo);
            }
        }
    }

    protected void supplmentDelayAvg(TaskDto task, String taskId, Number number, AlarmRuleDto alarmRuleDto, String nodeId, String nodeName, String[] template) {
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
        } else if (Objects.isNull(infoMap)){
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
                .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(task.getAgentId()).taskId(taskId).desc(task.getDesc())
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
            if (first.isPresent()) {
                AlarmInfo data = first.get();
                alarmInfo.setId(data.getId());

                long continued = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - data.getFirstOccurrenceTime().getTime());
                if (continued > 0) {
                    param.put("continueTime", continued);
                    summary = template[3];
                } else {
                    summary = template[2];
                }

            } else {
                summary = template[2];
            }
            alarmInfo.setLastOccurrenceTime(null);
            alarmInfo.setStatus(AlarmStatusEnum.ING);
            alarmInfo.setFirstOccurrenceTime(null);
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
                alarmInfo.setStatus(AlarmStatusEnum.RECOVER);
                alarmService.save(alarmInfo);
            }
        }
    }

    private String[] getTemplate(AlarmKeyEnum alarmKeyEnum, Boolean source) {
        String[] result = null;
        if (alarmKeyEnum == AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME) {
            if (source) {
                result = new String[]{AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME.name(), MetricCons.SS.VS.F_SNAPSHOT_SOURCE_READ_TIME_COST_AVG,
                        "DATANODE_AVERAGE_HANDLE_CONSUME_START",
                        "DATANODE_AVERAGE_HANDLE_CONSUME_ALWAYS",
                        "DATANODE_AVERAGE_HANDLE_CONSUME_RECOVER"};
            } else {
                result = new String[]{AlarmKeyEnum.DATANODE_AVERAGE_HANDLE_CONSUME.name(), MetricCons.SS.VS.F_TARGET_WRITE_TIME_COST_AVG,
                        "TARGET_AVERAGE_HANDLE_CONSUME_START",
                        "TARGET_AVERAGE_HANDLE_CONSUME_ALWAYS",
                        "TARGET_AVERAGE_HANDLE_CONSUME_RECOVER"};
            }
        } else if (alarmKeyEnum == AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME) {
            result = new String[]{AlarmKeyEnum.PROCESSNODE_AVERAGE_HANDLE_CONSUME.name(), MetricCons.SS.VS.F_TIME_COST_AVG,
                    "PROCESSNODE_AVERAGE_HANDLE_CONSUME_START",
                    "PROCESSNODE_AVERAGE_HANDLE_CONSUME_ALWAYS",
                    "PROCESSNODE_AVERAGE_HANDLE_CONSUME_RECOVER"};
        }

        return result;
    }

    public void removeObsInfoByTaskId(String taskId) {
        obsMap.remove(taskId);
    }

    public void removeObsInfoByTaskIdAndKey(String taskId, String key) {
        if (obsMap.containsKey(taskId)) {
            Map<String, AtomicInteger> integerMap = obsMap.get(taskId);
            if (Objects.nonNull(integerMap)) {
                integerMap.remove(key);
            }
        }
    }
}
