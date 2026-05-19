package com.tapdata.tm.schedule;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.alarm.AlarmComponentEnum;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskAlarmScheduler {

    private TaskService taskService;
    private AlarmService alarmService;
    private WorkerService workerService;
    private UserService userService;
    private SettingsService settingsService;
    private AgentGroupService agentGroupService;
    private MongoTemplate mongoTemplate;

    @Scheduled(fixedDelay = 30000)
    @SchedulerLock(name ="task_agent_alarm_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void taskAgentAlarm() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-taskAgentAlarm");
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
            try {
                UserDetail userDetail = userDetailMap.get(data.getUserId());
                boolean checkOpen = alarmService.checkOpen(data, null, AlarmKeyEnum.SYSTEM_FLOW_EGINGE_DOWN, null, userDetail);
                if (!checkOpen) {
                    continue;
                }

                List<Worker> workerList = findWorkerList(data, userDetail);

                String orginAgentId = data.getAgentId();
                AtomicReference<String> summary = new AtomicReference<>();
                Map<String, Object> param = Maps.newHashMap();
                String alarmDate = DateUtil.now();
                param.put("alarmDate", alarmDate);
                param.put("taskName", data.getName());
                param.put("agentName", orginAgentId);
                if (CollectionUtils.isEmpty(workerList)) {
                    FunctionUtils.isTureOrFalse(isCloud).trueOrFalseHandle(
                            () -> summary.set("SYSTEM_FLOW_EGINGE_DOWN_CLOUD"),
                            () -> summary.set("SYSTEM_FLOW_EGINGE_DOWN_NO_AGENT")
                    );
                } else {
                    summary.set("SYSTEM_FLOW_EGINGE_DOWN_CLOUD");
                }

                AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.WARNING).component(AlarmComponentEnum.FE)
                        .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(orginAgentId).taskId(data.getId().toHexString())
                        .name(data.getName()).summary(summary.get()).metric(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_DOWN)
                        .build();
                alarmInfo.setParam(param);
                alarmInfo.setUserId(data.getUserId());
                alarmService.save(alarmInfo);
            } catch (Exception e) {
                log.error("taskAgentAlarm: failed to handle agent alarm for task [{}]: {}", data.getName(), e.getMessage(), e);
            }
        }
    }

    @Scheduled(cron = "0 0/1 * * * ? ")
    @SchedulerLock(name = "engine_heartbeat_alarm_lock", lockAtMostFor = "30s", lockAtLeastFor = "10s")
    public void engineHeartbeatAlarm() {
        Thread.currentThread().setName(getClass().getSimpleName() + "-engineHeartbeatAlarm");

        Query workerQuery = new Query(Criteria.where("worker_type").is("connector"));
        List<WorkerDto> workers = workerService.findAll(workerQuery);
        if (CollectionUtils.isEmpty(workers)) {
            return;
        }

        Object heartTime = settingsService.getValueByCategoryAndKey(CategoryEnum.WORKER, KeyEnum.WORKER_HEART_TIMEOUT);
        long heartExpire = Objects.nonNull(heartTime) ? (Long.parseLong(heartTime.toString()) + 48) * 1000 : 108000;
        long now = System.currentTimeMillis();

        Map<String, WorkerDto> offlineMap = workers.stream()
                .filter(w -> (Objects.nonNull(w.getIsDeleted()) && w.getIsDeleted())
                        || (Objects.nonNull(w.getStopping()) && w.getStopping())
                        || (Objects.nonNull(w.getPingTime()) && w.getPingTime() < (now - heartExpire)))
                .collect(Collectors.toMap(WorkerDto::getProcessId, Function.identity(), (e1, e2) -> e1));

        Set<String> onlineAgentIds = workers.stream()
                .map(WorkerDto::getProcessId)
                .filter(id -> StringUtils.isNotBlank(id) && !offlineMap.containsKey(id))
                .collect(Collectors.toSet());

        Query ingOfflineQuery = new Query(Criteria.where("status").is(AlarmStatusEnum.ING)
                .and("metric").is(AlarmKeyEnum.ENGINE_OFFLINE.name()));
        List<AlarmInfo> ingOffline = mongoTemplate.find(ingOfflineQuery, AlarmInfo.class);
        Set<String> alreadyAlarmedAgentIds = ingOffline.stream()
                .map(AlarmInfo::getAgentId).filter(Objects::nonNull).collect(Collectors.toSet());

        Date currentDate = new Date();
        String alarmDate = DateUtil.now();

        if (!offlineMap.isEmpty()) {
            for (Map.Entry<String, WorkerDto> entry : offlineMap.entrySet()) {
                String agentId = entry.getKey();
                if (alreadyAlarmedAgentIds.contains(agentId)) {
                    continue;
                }
                WorkerDto worker = entry.getValue();
                createEngineOfflineAlarm(agentId, worker);
            }
        }

        Map<String, AlarmInfo> offlineByAgent = ingOffline.stream()
                .filter(a -> StringUtils.isNotBlank(a.getAgentId()))
                .collect(Collectors.toMap(AlarmInfo::getAgentId, Function.identity(), (a, b) -> a));
        Map<String, WorkerDto> workerByAgent = workers.stream()
                .filter(w -> StringUtils.isNotBlank(w.getProcessId()))
                .collect(Collectors.toMap(WorkerDto::getProcessId, Function.identity(), (a, b) -> a));

        for (Map.Entry<String, AlarmInfo> entry : offlineByAgent.entrySet()) {
            String agentId = entry.getKey();
            if (!onlineAgentIds.contains(agentId)) {
                continue;
            }
            mongoTemplate.updateMulti(
                    Query.query(Criteria.where("status").is(AlarmStatusEnum.ING)
                            .and("metric").is(AlarmKeyEnum.ENGINE_OFFLINE.name())
                            .and("agentId").is(agentId)),
                    new Update().set("status", AlarmStatusEnum.RECOVER).set("recoveryTime", currentDate),
                    AlarmInfo.class);

            WorkerDto live = workerByAgent.get(agentId);
            String agentName = live != null && StringUtils.isNotBlank(live.getHostname())
                    ? live.getHostname()
                    : (StringUtils.isNotBlank(entry.getValue().getName()) ? entry.getValue().getName() : agentId);
            Map<String, Object> param = Maps.newHashMap();
            param.put("agentName", agentName);
            param.put("agentId", agentId);
            param.put("alarmDate", alarmDate);
            AlarmInfo info = AlarmInfo.builder()
                    .status(AlarmStatusEnum.ING).level(Level.RECOVERY)
                    .component(AlarmComponentEnum.FE).type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM)
                    .agentId(agentId).name(agentName).summary("ENGINE_ONLINE")
                    .metric(AlarmKeyEnum.ENGINE_ONLINE).build();
            info.setParam(param);
            info.setFirstOccurrenceTime(currentDate);
            info.setLastOccurrenceTime(currentDate);
            info.setLastNotifyTime(currentDate);
            mongoTemplate.insert(info);
            log.info("Engine online recovered agentId={}, agentName={}", agentId, agentName);
        }
    }

    @Scheduled(fixedDelay = 30000)
    @SchedulerLock(name ="task_increment_delay_cleanup_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void cleanupTaskIncrementDelayAlarm() {
        Thread.currentThread().setName(getClass().getSimpleName() + "-cleanupTaskIncrementDelayAlarm");
        Query query = new Query(Criteria.where("taskIncrementDelay").ne(null)
                .and("status").is(TaskDto.STATUS_RUNNING)
                .and("is_deleted").is(false));
        query.fields().include("_id", "delayTime", "taskIncrementDelay", "taskIncrementDelayThreshold");
        List<TaskDto> taskDtos = taskService.findAll(query);
        if (CollectionUtils.isEmpty(taskDtos)) {
            return;
        }
        for (TaskDto task : taskDtos) {
            Long delayTime = task.getDelayTime();
            Long threshold = task.getTaskIncrementDelayThreshold();
            if (null == threshold) {
                continue;
            }
            if (null == delayTime || delayTime < threshold) {
                taskService.updateTaskIncrementDelayAlarm(task.getId(),null,null);
            }
        }
    }

    protected List<Worker> findWorkerList(TaskDto data, UserDetail userDetail) {
        if (null == workerService) {
            return Lists.newArrayList();
        }
        List<Worker> workerList = workerService.findAvailableAgentBySystem(userDetail);
        if (null == data) {
            return workerList;
        }
        if (null == agentGroupService) {
            return workerList;
        }
        if (AccessNodeTypeEnum.isManually(data.getAccessNodeType())) {
            List<String> processIdList = agentGroupService.getProcessNodeListWithGroup(data, userDetail);
            workerList = workerList.stream().filter(w -> processIdList.contains(w.getProcessId())).collect(Collectors.toList());
        }
        return workerList;
    }

    public boolean createEngineOfflineAlarm(String processId, WorkerDto worker) {
        if (StringUtils.isBlank(processId)) {
            log.warn("createEngineOfflineAlarm: processId is blank");
            return false;
        }

        try {
            Query alreadyAlarmedQuery = new Query(Criteria.where("status").is(AlarmStatusEnum.ING)
                    .and("metric").is(AlarmKeyEnum.ENGINE_OFFLINE.name())
                    .and("agentId").is(processId));
            long alarmCount = mongoTemplate.count(alreadyAlarmedQuery, AlarmInfo.class);
            if (alarmCount > 0) {
                log.debug("createEngineOfflineAlarm: alarm already exists processId={}", processId);
                return false;
            }

            if (worker == null) {
                Query workerQuery = Query.query(Criteria.where("process_id").is(processId)
                        .and("worker_type").is("connector"));
                worker = mongoTemplate.findOne(workerQuery, WorkerDto.class, "Workers");
                if (worker == null) {
                    log.warn("createEngineOfflineAlarm: worker not found processId={}", processId);
                    worker = new WorkerDto();
                    worker.setProcessId(processId);
                }
            }

            Query runningTaskQuery = new Query(Criteria.where("agentId").is(processId)
                    .and("status").is(TaskDto.STATUS_RUNNING)
                    .and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                    .and("is_deleted").is(false));
            long taskCount = taskService.count(runningTaskQuery);

            String agentName = StringUtils.isNotBlank(worker.getHostname()) ? worker.getHostname() : processId;
            Date currentDate = new Date();
            String alarmDate = DateUtil.now();

            Map<String, Object> param = Maps.newHashMap();
            param.put("agentName", agentName);
            param.put("agentId", processId);
            param.put("taskCount", taskCount);
            param.put("alarmDate", alarmDate);

            AlarmInfo alarmInfo = AlarmInfo.builder()
                    .status(AlarmStatusEnum.ING)
                    .level(Level.WARNING)
                    .component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM)
                    .agentId(processId)
                    .name(agentName)
                    .summary("ENGINE_OFFLINE")
                    .metric(AlarmKeyEnum.ENGINE_OFFLINE)
                    .build();
            alarmInfo.setParam(param);
            alarmInfo.setFirstOccurrenceTime(currentDate);
            alarmInfo.setLastOccurrenceTime(currentDate);
            alarmInfo.setLastNotifyTime(currentDate);

            mongoTemplate.insert(alarmInfo);
            log.warn("Engine offline alarm created agentId={} agentName={} taskCount={}", processId, agentName, taskCount);
            return true;
        } catch (Exception e) {
            log.error("createEngineOfflineAlarm failed processId={}", processId, e);
            return false;
        }
    }
}
