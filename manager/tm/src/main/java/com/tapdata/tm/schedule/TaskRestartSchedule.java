package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TransformSchemaService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2022/2/9
 * @Description:
 */
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskRestartSchedule {

    private TaskService taskService;
    private TaskScheduleService taskScheduleService;
    private UserService userService;
    private SettingsService settingsService;
    private WorkerService workerService;
    private MonitoringLogsService monitoringLogsService;
    private StateMachineService stateMachineService;
    private TransformSchemaService transformSchema;
    private MetadataDefinitionService metadataDefinitionService;

    /**
     * 定时重启任务，只要找到有重启标记，并且是停止状态的任务，就重启，每分钟启动一次
     */
    @Scheduled(fixedDelay = 60 * 1000)
    @SchedulerLock(name ="restartTask_lock", lockAtMostFor = "5s", lockAtLeastFor = "5s")
    public void restartTask() {
        Thread.currentThread().setName("taskSchedule-restartTask");
        //查询到所有需要重启的任务
        Criteria criteria = Criteria.where("restartFlag").is(true).and("status").is(TaskDto.STATUS_STOP);
        Query query = new Query(criteria);
        query.fields().include("_id", "restartUserId");
        List<TaskDto> restartTasks = taskService.findAll(query);
        for (TaskDto task : restartTasks) {

            try {
                UserDetail user = userService.loadUserById(MongoUtils.toObjectId(task.getRestartUserId()));
                transformSchema.transformSchemaBeforeDynamicTableName(task, user);
                taskService.start(task.getId(), user, true);
            } catch (Exception e) {
                log.warn("restart subtask error, task id = {}, e = {}", task.getId(), e.getMessage());
            }
        }
    }

    @Scheduled(initialDelay = 10 * 1000, fixedDelay = 10 * 1000)
    @SchedulerLock(name ="engineRestartNeedStartTask_lock2", lockAtMostFor = "30s", lockAtLeastFor = "5s")
    public void engineRestartNeedStartTask() {
        Thread.currentThread().setName("taskSchedule-engineRestartNeedStartTask");

        //云版不需要这个重新调度的逻辑
        boolean isCloud = isCloud();
        long heartExpire = getHeartExpire();

        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_RUNNING)
                .and("pingTime").lt(System.currentTimeMillis() - heartExpire);
        List<TaskDto> all = taskService.findAll(new Query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        } else {
            log.info("engineRestartNeedStartTask task size {}", all.size());
        }

        Map<String, UserDetail> userDetailMap = getUserDetailMap(all);
        Map<String, List<Worker>> userWorkerMap = this.getUserWorkMap();
        List<TaskDto> orderTask = metadataDefinitionService.orderTaskByTagPriority(all);
        for (TaskDto taskDto : orderTask) {
            UserDetail user = userDetailMap.get(taskDto.getUserId());
            if (null == user || restartInCloud(isCloud, taskDto, user)) continue;

            // The criteria already selected this RUNNING task because its task.pingTime is stale.
            // Re-confirm with a fresh read before demoting it: the engine refreshes task.pingTime
            // only for tasks it is actually executing, so a fresh pingTime here means the engine
            // is genuinely running it (the batch list was momentarily behind) and it must be left
            // alone. A bare "worker process alive" signal is NOT proof the task is running, and is
            // also unreliable on single-node DAAS (user-scoped worker lookup / blank agentId).
            if (isTaskActiveOnAgent(taskDto, heartExpire)) {
                logSkipReschedule("engineRestartNeedStartTask", taskDto, "running_and_agent_pinging");
                continue;
            }

            List<Worker> workerList = getUserWorkList(isCloud, userWorkerMap, user.getUserId());
            if (CollectionUtils.isNotEmpty(workerList)) {
                StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, user);
                if (stateMachineResult.isOk()) {
                    transformSchema.transformSchemaBeforeDynamicTableName(taskDto, user);
                    taskScheduleService.scheduling(taskDto, user,true);
                }
            }
        }
    }

    /**
     * Returns true iff the engine is actually executing the task right now — i.e. its
     * task-level heartbeat ({@code task.pingTime}) has been refreshed within {@code heartExpire}.
     * {@code TaskPingTimeMonitor} pings {@code pingTime} every 5s ONLY for tasks the engine is
     * running, so a fresh {@code pingTime} is direct evidence the assigned engine holds the task —
     * unlike "worker process alive", which says nothing about whether this particular task is
     * being executed (a stuck task shows no thread on the engine). Queried in MongoDB because
     * {@code pingTime} is persisted as an epoch-millis {@code long}; reading the {@code Date}
     * entity getter would mis-map it.
     */
    private boolean isTaskActiveOnAgent(TaskDto taskDto, long heartExpire) {
        if (Objects.isNull(taskDto) || Objects.isNull(taskDto.getId())) {
            return false;
        }
        Query query = Query.query(Criteria.where("_id").is(taskDto.getId())
                .and(TaskDto.PING_TIME_FIELD).gte(System.currentTimeMillis() - heartExpire));
        return taskService.count(query) > 0;
    }

    private final AtomicLong lastSkipWarnAt = new AtomicLong(0L);

    private void logSkipReschedule(String scheduler, TaskDto taskDto, String reason) {
        // Day-throttled warn — without this, every 10s scan that finds the same false-positive
        // would spam the log with the same message (≥ 8640 lines/day per affected task).
        warnThrottled(lastSkipWarnAt,
                () -> log.warn("TaskHA event=skip_reschedule scheduler={} taskId={} agentId={} reason={}",
                        scheduler, taskDto.getId().toHexString(), taskDto.getAgentId(), reason));
    }

    @NotNull
    private Map<String, UserDetail> getUserDetailMap(List<TaskDto> all) {
        List<String> userIds = all.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
        List<UserDetail> userByIdList = userService.getUserByIdList(userIds);
        return userByIdList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity(), (e1, e2) -> e1));
    }

    /**
     * Lower bound covers one missed 5s ping plus a 5s HTTP timeout — going below
     * this risks false-positive failover on transient network hiccups.
     */
    static final long MIN_JOB_HEART_TIMEOUT_MS = 25_000L;
    private static final long WARN_LOG_INTERVAL_MS = 24 * 60 * 60 * 1000L;
    private final AtomicLong lastMissingSettingWarnAt = new AtomicLong(0L);
    private final AtomicLong lastClampWarnAt = new AtomicLong(0L);

    private long getHeartExpire() {
        long heartExpire;
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.JOB, KeyEnum.JOB_HEART_TIMEOUT);
        if (Objects.nonNull(settings) && Objects.nonNull(settings.getValue())) {
            heartExpire = Long.parseLong(settings.getValue().toString());
        } else {
            heartExpire = 300000L;
            warnThrottled(lastMissingSettingWarnAt,
                    () -> log.warn("getHeartExpire: JOB_HEART_TIMEOUT not found in Settings, using default {}ms", 300000L));
        }
        if (heartExpire < MIN_JOB_HEART_TIMEOUT_MS) {
            long original = heartExpire;
            heartExpire = MIN_JOB_HEART_TIMEOUT_MS;
            warnThrottled(lastClampWarnAt,
                    () -> log.warn("getHeartExpire: configured value {}ms is below minimum {}ms, clamping",
                            original, MIN_JOB_HEART_TIMEOUT_MS));
        }
        return heartExpire;
    }

    private void warnThrottled(AtomicLong lastAt, Runnable logFn) {
        long now = System.currentTimeMillis();
        long prev = lastAt.get();
        if (now - prev >= WARN_LOG_INTERVAL_MS && lastAt.compareAndSet(prev, now)) {
            logFn.run();
        }
    }



    /**
     * 对于少量因为tm线程终止导致的任务一直启动中（页面的直观显示），也就是调度中状态。做一些定时任务补救措施
     */
    @Scheduled(fixedDelay = 30 * 1000)
    @SchedulerLock(name ="schedulingTask_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void overTimeTask() {
        Thread.currentThread().setName("taskSchedule-schedulingTask");
        FunctionUtils.ignoreAnyError(this::stoppingTask);
        FunctionUtils.ignoreAnyError(this::schedulingTask);
        FunctionUtils.ignoreAnyError(this::waitRunTask);
    }

    public void stoppingTask() {
        long overTime = 30000L;
        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_STOPPING)
                .and("stopRetryTimes").lt(8)
                .and("last_updated").lt(new Date(System.currentTimeMillis() - overTime));
        List<TaskDto> all = taskService.findAll(Query.query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        List<String> userList = all.stream().map(BaseDto::getUserId).collect(Collectors.toList());
        Map<String, UserDetail> userMap = userService.getUserMapByIdList(userList);

        for (TaskDto taskDto : all) {
            int stopRetryTimes = taskDto.getStopRetryTimes();
            UserDetail userDetail = userMap.get(taskDto.getUserId());
            if (Objects.isNull(userDetail)) {
                continue;
            }

            if (stopRetryTimes >= 7) {
                CompletableFuture.runAsync(() -> {
                    String template = "The task is being stopped, the number of retries is {0}, it is recommended to try to force stop.";
                    String msg = MessageFormat.format(template, taskDto.getStopRetryTimes());
                    monitoringLogsService.startTaskErrorLog(taskDto, userDetail, msg, Level.WARN);
                });
                stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, userDetail);
            } else {
                taskService.sendStoppingMsg(taskDto.getId().toHexString(), taskDto.getAgentId(), userDetail, false);
                Update update = Update.update("stopRetryTimes", taskDto.getStopRetryTimes() + 1).set("last_updated", taskDto.getLastUpdAt());
                taskService.updateById(taskDto.getId(), update, userDetail);
            }
        }
    }

    public void schedulingTask() {
        long timeout = 30000L;
        long heartExpire = getHeartExpire();
        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_SCHEDULING)
            .and("schedulingTime").lt(new Date(System.currentTimeMillis() - timeout));
        List<TaskDto> all = taskService.findAll(Query.query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        Map<String, UserDetail> userMap = this.getUserDetailMap(all);
        for (TaskDto taskDto : all) {
            String agentId = taskDto.getAgentId();
            UserDetail user = userMap.get(taskDto.getUserId());
            if (Objects.isNull(user)
                || (isCloud() && skipCloudEngineOffline(agentId, user))) {
                continue;
            }

            if (!isCloud()) {
                // Non-cloud: re-dispatch unless the assigned engine is actually executing this task
                // (fresh task.pingTime). A live worker *process* is NOT proof the task is running
                // (a stuck task shows no thread on the engine). Shared with the engine-online trigger.
                reDispatchIfIdle(taskDto, user, "schedulingTask");
                continue;
            }

            // Cloud: preserve the existing timeout-based behavior.
            if (Objects.nonNull(taskDto.getSchedulingTime()) && (
                System.currentTimeMillis() - taskDto.getSchedulingTime().getTime() > heartExpire)) {
                asyncTaskWarnLog(taskDto, user, "The engine[{0}] takes over the task with a timeout of {1}ms."
                    , agentId, timeout
                );
                stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
            } else {
                transformSchema.transformSchemaBeforeDynamicTableName(taskDto, user);
                taskScheduleService.scheduling(taskDto, user, true);
            }
        }
    }

    /**
     * Immediately re-dispatch the tasks parked in SCHEDULING that belong to {@code agentId}
     * (or are unassigned), so a just-restarted engine takes them over without waiting for the
     * periodic poll. Returns the number of idle parked tasks it (re)dispatched.
     */
    public int reDispatchSchedulingTasksOf(String agentId) {
        if (agentId == null || agentId.trim().isEmpty()) {
            return 0;
        }
        // Parked tasks the restarted engine should reclaim: still SCHEDULING and either assigned to
        // this agent (manually-pinned tasks keep agentId) or unassigned (automatic tasks get agentId
        // cleared when the stop-time scheduling() found no live worker — see cloudTaskLimitNum).
        Criteria criteria = new Criteria().andOperator(
                Criteria.where("status").is(TaskDto.STATUS_SCHEDULING),
                new Criteria().orOperator(
                        Criteria.where("agentId").is(agentId),
                        Criteria.where("agentId").in(null, "")
                )
        );
        List<TaskDto> all = taskService.findAll(Query.query(criteria));
        if (CollectionUtils.isEmpty(all)) {
            return 0;
        }
        Map<String, UserDetail> userMap = getUserDetailMap(all);
        int redispatched = 0;
        for (TaskDto taskDto : all) {
            UserDetail user = userMap.get(taskDto.getUserId());
            if (Objects.isNull(user)) {
                continue;
            }
            if (reDispatchIfIdle(taskDto, user, "engineOnline")) {
                redispatched++;
            }
        }
        return redispatched;
    }

    /**
     * "Actively running now" window for task.pingTime. The engine's TaskPingTimeMonitor pings every
     * 5s ONLY for tasks it is executing, so a pingTime within a few intervals means the engine truly
     * holds the task. Deliberately far smaller than heartExpire (the ~5-min death threshold): using
     * heartExpire here would make a task the engine JUST stopped (graceful handover) look "active"
     * for up to ~5 min and block its re-dispatch — i.e. it would only recover after the task timeout.
     */
    static final long TASK_ACTIVE_PING_WINDOW_MS = 15_000L;

    /**
     * Re-dispatch a single SCHEDULING task unless the assigned engine is actually executing it
     * (pingTime refreshed within TASK_ACTIVE_PING_WINDOW_MS). Returns true iff it was (re)dispatched.
     * Shared by schedulingTask() and the engine-online trigger so the "is the engine running it?"
     * decision lives in one place.
     */
    private boolean reDispatchIfIdle(TaskDto taskDto, UserDetail user, String scheduler) {
        if (isTaskActiveOnAgent(taskDto, TASK_ACTIVE_PING_WINDOW_MS)) {
            logSkipReschedule(scheduler, taskDto, "scheduling_but_agent_running_task");
            return false;
        }
        // scheduling() re-selects an agent and moves SCHEDULING->WAIT_RUN; if none is available it
        // keeps the task in SCHEDULING for the next round. Guarded so one failure can't abort a scan.
        FunctionUtils.ignoreAnyError(() -> {
            transformSchema.transformSchemaBeforeDynamicTableName(taskDto, user);
            taskScheduleService.scheduling(taskDto, user, true);
        });
        return true;
    }

    private static final int ENGINE_ONLINE_MAX_ATTEMPTS = 6;
    private static final long ENGINE_ONLINE_RETRY_DELAY_MS = 2000L;
    private final ScheduledExecutorService engineOnlineExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "engine-online-redispatch");
                t.setDaemon(true);
                return t;
            });

    /**
     * Fired when an engine (re)connects (TM WebSocketServer.afterConnectionEstablished): immediately
     * re-dispatch its parked SCHEDULING tasks so a just-restarted engine takes them over without
     * waiting for the 30s poll. Runs off the caller's thread with a few bounded retries, because the
     * WS reconnect can land before the restarted worker's first heartbeat refreshes ping_time (so
     * cloudTaskLimitNum can select it). The 30s schedulingTask poll remains the guaranteed backstop.
     */
    public void onEngineOnlineAsync(String agentId) {
        if (agentId == null || agentId.trim().isEmpty() || isCloud()) {
            return;
        }
        scheduleEngineOnlineAttempt(agentId, 0);
    }

    private void scheduleEngineOnlineAttempt(String agentId, int attempt) {
        long delay = attempt == 0 ? 0L : ENGINE_ONLINE_RETRY_DELAY_MS;
        try {
            engineOnlineExecutor.schedule(() -> {
                int redispatched = 0;
                try {
                    redispatched = reDispatchSchedulingTasksOf(agentId);
                } catch (Throwable t) {
                    log.warn("TaskHA event=engine_online_redispatch_failed agentId={} attempt={} err={}", agentId, attempt, t.getMessage());
                }
                // Keep retrying while idle parked tasks remain (the worker may not be selectable yet);
                // stop once none remain or attempts are exhausted (30s poll is the final backstop).
                if (redispatched > 0 && attempt + 1 < ENGINE_ONLINE_MAX_ATTEMPTS) {
                    scheduleEngineOnlineAttempt(agentId, attempt + 1);
                }
            }, delay, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("TaskHA event=engine_online_schedule_failed agentId={} err={}", agentId, e.getMessage());
        }
    }

    public void waitRunTask() {
        long heartExpire = getHeartExpire();
        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_WAIT_RUN)
                .and("scheduledTime").lt(new Date(System.currentTimeMillis() - heartExpire));
        List<TaskDto> all = taskService.findAll(Query.query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        Map<String, UserDetail> userMap = this.getUserDetailMap(all);
        for (TaskDto taskDto : all) {
            String agentId = taskDto.getAgentId();
            UserDetail user = userMap.get(taskDto.getUserId());
            if (Objects.isNull(user)
                || (isCloud() && skipCloudEngineOffline(agentId, user))) {
                continue;
            }

            asyncTaskWarnLog(taskDto, user, "The engine[{0}] takes over the task with a timeout of {1}ms."
                , agentId, heartExpire
            );
            StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, user);
            if (stateMachineResult.isOk()) {
                try {
                    asyncTaskWarnLog(taskDto, user, "In the process of rescheduling tasks, the scheduling engine is {0}."
                        , agentId
                    );
                    taskScheduleService.scheduling(taskDto, user,true);
                } catch (Exception e) {
                    monitoringLogsService.startTaskErrorLog(taskDto, user, e, Level.ERROR);
                    throw e;
                }
            }
        }
    }

    private Map<String, List<Worker>> getUserWorkMap() {
        List<Worker> workers = workerService.findAvailableAgentBySystem(Collections.emptyList());
        AtomicReference<Map<String, List<Worker>>> userWorkerMap = new AtomicReference<>();
        Optional.ofNullable(workers).ifPresent(list -> userWorkerMap.set(list.stream().collect(Collectors.groupingBy(Worker::getUserId))));

        return userWorkerMap.get();
    }

    private boolean isCloud() {
        //云版不需要这个重新调度的逻辑
        Object buildProfile = settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE);
        if (Objects.isNull(buildProfile)) {
            buildProfile = "DAAS";
        }
        return buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");
    }

    protected boolean skipCloudEngineOffline(String agentId, UserDetail user) {
        // 云版，只在引擎在线时重新调度
        String status = workerService.checkUsedAgent(agentId, user);
        if ("online".equals(status)) return false;

        log.debug("The cloud version does not need this rescheduling, engine: '{}', status {}", agentId, status);
        return true;
    }

    private boolean restartInCloud(boolean isCloud, TaskDto taskDto, UserDetail user) {
        if (isCloud) {
            String status = workerService.checkUsedAgent(taskDto.getAgentId(), user);
            if ("offline".equals(status)) {
                log.debug("The cloud version does not need this rescheduling");
                return true;
            } else if ("online".equals(status)) {
                taskScheduleService.sendStartMsg(taskDto.getId().toHexString(), taskDto.getAgentId(), user);
                return true;
            }
        }
        return false;
    }

    private List<Worker> getUserWorkList(boolean isCloud, Map<String, List<Worker>> userWorkerMap, String userId) {
        if (isCloud) {
            return userWorkerMap.get(userId);
        } else {
            return userWorkerMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        }
    }

    @Scheduled(fixedDelay = 60000)
    @SchedulerLock(name = "retrySchedulingFailedTask_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void retrySchedulingFailedTask() {
        Thread.currentThread().setName("taskSchedule-retrySchedulingFailed");
        if (isCloud()) return;

        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_SCHEDULE_FAILED)
                .and("last_updated").lt(new Date(System.currentTimeMillis() - 60000L));
        List<TaskDto> all = taskService.findAll(Query.query(criteria));
        if (CollectionUtils.isEmpty(all)) return;

        Map<String, UserDetail> userDetailMap = getUserDetailMap(all);
        Map<String, List<Worker>> userWorkerMap = this.getUserWorkMap();
        if (userWorkerMap == null || userWorkerMap.isEmpty()) return;

        for (TaskDto taskDto : all) {
            UserDetail user = userDetailMap.get(taskDto.getUserId());
            if (null == user) continue;
            List<Worker> workerList = getUserWorkList(false, userWorkerMap, user.getUserId());
            if (CollectionUtils.isNotEmpty(workerList)) {
                StateMachineResult result = stateMachineService.executeAboutTask(
                        taskDto, DataFlowEvent.OVERTIME, user);
                if (result.isOk()) {
                    log.info("Auto-retrying scheduling_failed task [{}]", taskDto.getName());
                    transformSchema.transformSchemaBeforeDynamicTableName(taskDto, user);
                    taskScheduleService.scheduling(taskDto, user, true);
                }
            }
        }
    }

    protected void asyncTaskWarnLog(TaskDto taskDto, UserDetail user, String template, Object... arguments) {
        CompletableFuture.runAsync(() -> {
            String msg = MessageFormat.format(template, arguments);
            monitoringLogsService.startTaskErrorLog(taskDto, user, msg, Level.WARN);
        });
    }

}
