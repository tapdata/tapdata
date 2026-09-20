package com.tapdata.tm.schedule;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.alarm.AlarmComponentEnum;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TransformSchemaService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Detects stalled incremental (CDC) progress on running tasks and triggers a single fallback restart.
 *
 * <p>Monitored unit = each {@code attrs.syncProgress} entry (a source&rarr;target node-pair). A unit is
 * monitored only while it is in CDC stage ({@code syncStage == "CDC"}); snapshot / initial-sync units
 * are skipped. {@code logCollector} (mining) tasks may not carry a CDC stage, so their units are
 * monitored by their streaming progress signal ({@code sourceTime} / {@code eventTime}) instead.
 *
 * <p>Progress is diffed per unit against the last observed signature
 * ({@code syncStage / streamOffset / offset / eventTime / sourceTime}); a unit only refreshes its own
 * {@code lastProgressAt}, so progress in one unit does not mask a stall in another. State is kept under
 * {@code attrs.incrementStuck.&lt;unitKey&gt;} so it survives a TM restart. When a unit stalls past the
 * configured timeout the owning task is restarted through the existing state machine
 * ({@code OVERTIME} &rarr; {@code scheduling}), guarded by a per-unit cooldown and restart cap.
 */
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskIncrementStuckSchedule {
    private static final String SYNC_PROGRESS = "syncProgress";
    private static final String INCREMENT_STUCK = "incrementStuck";
    private static final String CDC = "CDC";

    private static final String FIELD_SYNC_STAGE = "syncStage";
    private static final String FIELD_STREAM_OFFSET = "streamOffset";
    private static final String FIELD_OFFSET = "offset";
    private static final String FIELD_EVENT_TIME = "eventTime";
    private static final String FIELD_SOURCE_TIME = "sourceTime";

    private static final String STATE_LAST_PROGRESS_AT = "lastProgressAt";
    private static final String STATE_LAST_SIGNATURE = "lastSignature";
    private static final String STATE_RESTART_COUNT = "restartCount";
    private static final String STATE_LAST_RESTART_AT = "lastRestartAt";

    private static final long DEFAULT_TIMEOUT_MS = 10 * 60 * 1000L;
    private static final long DEFAULT_COOLDOWN_MS = 2 * 60 * 60 * 1000L;
    private static final long DEFAULT_GRACE_MS = 15 * 60 * 1000L;
    private static final int DEFAULT_MAX_RESTARTS = 3;

    private TaskService taskService;
    private TaskScheduleService taskScheduleService;
    private StateMachineService stateMachineService;
    private TransformSchemaService transformSchemaService;
    private UserService userService;
    private MonitoringLogsService monitoringLogsService;
    private AlarmService alarmService;

    @Scheduled(fixedDelay = 5 * 60 * 1000L)
    // The scan iterates every running task with its full attrs and performs per-unit reads/writes.
    // lockAtMostFor must comfortably exceed a full scan so the lock cannot expire mid-run (which
    // would let a second node run concurrently and double-restart / lose incrementStuck updates).
    // A full scan should finish well within one 5-minute interval; 30 minutes leaves ample headroom
    // for a large task set while still releasing the lock (and allowing failover) if a scan wedges.
    // Per-unit persistence (see persistStates) further narrows the concurrent-write blast radius.
    @SchedulerLock(name = "taskIncrementStuck_lock", lockAtMostFor = "30m", lockAtLeastFor = "30s")
    public void detectIncrementStuck() {
        Thread.currentThread().setName("taskSchedule-detectIncrementStuck");
        List<TaskDto> tasks = taskService.findAll(Query.query(Criteria.where("status").is(TaskDto.STATUS_RUNNING)
                .and("agentId").ne(null)));
        long now = System.currentTimeMillis();
        for (TaskDto task : tasks) {
            // One malformed task must not abort the whole scan (align with TaskRestartSchedule).
            try {
                if (isMonitored(task)) {
                    inspectTask(task, now);
                }
            } catch (Exception exception) {
                log.warn("Failed to inspect increment progress for task {}", task == null ? null : task.getId(), exception);
            }
        }
    }

    private boolean isMonitored(TaskDto task) {
        if (task == null || !TaskDto.STATUS_RUNNING.equals(task.getStatus()) || StringUtils.isBlank(task.getAgentId())) {
            return false;
        }
        if (TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(task.getSyncType())) {
            return true;
        }
        if (!TaskDto.SYNC_TYPE_SYNC.equals(task.getSyncType())
                && !TaskDto.SYNC_TYPE_MIGRATE.equals(task.getSyncType())
                && !TaskDto.SYNC_TYPE_CONN_HEARTBEAT.equals(task.getSyncType())) {
            return false;
        }
        return !Boolean.TRUE.equals(task.getShareCdcEnable())
                && !(task.getAttrs() != null && task.getAttrs().containsKey(TaskDto.ATTRS_USED_SHARE_CACHE));
    }

    @SuppressWarnings("unchecked")
    private void inspectTask(TaskDto task, long now) {
        Map<String, Object> attrs = task.getAttrs();
        if (attrs == null) {
            return;
        }
        Object progressObject = attrs.get(SYNC_PROGRESS);
        if (!(progressObject instanceof Map)) {
            return;
        }
        Map<String, Object> progress = (Map<String, Object>) progressObject;
        Map<String, Object> states = asMap(attrs.get(INCREMENT_STUCK));
        List<String> changedUnits = new ArrayList<>();
        List<String> stuckUnits = new ArrayList<>();
        for (Map.Entry<String, Object> entry : progress.entrySet()) {
            // The engine persists each unit's value as a JSON string of its SyncProgress object.
            Map<String, Object> unitProgress = parseProgress(entry.getValue());
            if (!isCdcUnit(task, unitProgress)) {
                continue;
            }
            String unit = entry.getKey();
            Map<String, Object> state = asMap(states.get(unit));
            String signature = progressSignature(unitProgress);
            if (state.isEmpty()) {
                // Seed without judging: prevents a restart storm on first deploy / newly started task.
                state.put(STATE_LAST_PROGRESS_AT, now);
                state.put(STATE_LAST_SIGNATURE, signature);
                state.put(STATE_RESTART_COUNT, 0);
                states.put(unit, state);
                changedUnits.add(unit);
                continue;
            }
            if (!signature.equals(String.valueOf(state.get(STATE_LAST_SIGNATURE)))) {
                state.put(STATE_LAST_PROGRESS_AT, now);
                state.put(STATE_LAST_SIGNATURE, signature);
                states.put(unit, state);
                changedUnits.add(unit);
                continue;
            }
            long lastProgressAt = asLong(state.get(STATE_LAST_PROGRESS_AT), now);
            if (now - lastProgressAt >= timeoutMs()) {
                stuckUnits.add(unit);
            }
        }
        if (!changedUnits.isEmpty()) {
            persistStates(task.getId(), states, changedUnits);
        }
        if (stuckUnits.isEmpty() || inGracePeriod(task, now)) {
            return;
        }
        restartIfAllowed(task, states, stuckUnits, now);
    }

    private void restartIfAllowed(TaskDto task, Map<String, Object> states, List<String> stuckUnits, long now) {
        List<String> eligibleUnits = new ArrayList<>();
        for (String unit : stuckUnits) {
            Map<String, Object> state = asMap(states.get(unit));
            int restartCount = (int) asLong(state.get(STATE_RESTART_COUNT), 0L);
            long lastRestartAt = asLong(state.get(STATE_LAST_RESTART_AT), 0L);
            if (restartCount < maxRestarts() && now - lastRestartAt >= cooldownMs()) {
                eligibleUnits.add(unit);
            }
        }
        String message = String.format("Increment progress stuck, task=%s, units=%s", task.getName(), stuckUnits);
        // Resolve the owner first: without it we can neither raise a user-scoped alarm nor restart.
        UserDetail user = loadUser(task);
        if (user == null) {
            log.warn("{}; task owner not found, skip alarm and restart", message);
            return;
        }
        // Record the stall before acting, so the alarm exists even if the restart is later suppressed.
        raiseStuckAlarm(task, user, states, stuckUnits, now);
        if (eligibleUnits.isEmpty()) {
            log.warn(message + ", automatic restart suppressed by cooldown or limit");
            monitoringLogsService.startTaskErrorLog(task, user, message + ", automatic restart suppressed by cooldown or limit", Level.WARN);
            return;
        }
        try {
            if (stateMachineService.executeAboutTask(task, DataFlowEvent.OVERTIME, user).isOk()) {
                transformSchemaService.transformSchemaBeforeDynamicTableName(task, user);
                taskScheduleService.scheduling(task, user, true);
                // Count the restart only after it actually succeeded (state machine accepted it).
                markRestarted(states, eligibleUnits, now);
                persistStates(task.getId(), states, eligibleUnits);
                log.warn(message + ", task restarted");
                monitoringLogsService.startTaskErrorLog(task, user, message + ", task restarted", Level.WARN);
            } else {
                log.warn(message + ", restart rejected by state machine, task not restarted");
                monitoringLogsService.startTaskErrorLog(task, user, message + ", restart rejected by state machine, task not restarted", Level.WARN);
            }
        } catch (Exception exception) {
            log.warn("Failed to restart increment-stuck task {}", task.getId(), exception);
            monitoringLogsService.startTaskErrorLog(task, user, message + ", restart failed: " + exception.getMessage(), Level.ERROR);
        }
    }

    private void markRestarted(Map<String, Object> states, List<String> units, long now) {
        for (String unit : units) {
            Map<String, Object> state = asMap(states.get(unit));
            state.put(STATE_RESTART_COUNT, (int) asLong(state.get(STATE_RESTART_COUNT), 0L) + 1);
            state.put(STATE_LAST_RESTART_AT, now);
            states.put(unit, state);
        }
    }

    private void raiseStuckAlarm(TaskDto task, UserDetail user, Map<String, Object> states,
                                 List<String> stuckUnits, long now) {
        try {
            if (!alarmService.checkOpen(task, null, AlarmKeyEnum.TASK_INCREMENT_STUCK, null, user)) {
                return;
            }
            long oldestProgressAt = now;
            for (String unit : stuckUnits) {
                oldestProgressAt = Math.min(oldestProgressAt, asLong(asMap(states.get(unit)).get(STATE_LAST_PROGRESS_AT), now));
            }
            Map<String, Object> param = Maps.newHashMap();
            param.put("taskName", task.getName());
            param.put("taskId", task.getId() == null ? null : task.getId().toHexString());
            param.put("syncType", task.getSyncType());
            param.put("units", String.join(",", stuckUnits));
            param.put("idleDuration", formatDuration(now - oldestProgressAt));
            param.put("lastProgressTime", DateUtil.formatDateTime(new Date(oldestProgressAt)));
            param.put("alarmDate", DateUtil.now());
            AlarmInfo alarmInfo = AlarmInfo.builder()
                    .status(AlarmStatusEnum.ING)
                    .level(Level.WARNING)
                    .component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM)
                    .agentId(task.getAgentId())
                    .taskId(task.getId() == null ? null : task.getId().toHexString())
                    .desc(task.getDesc())
                    .name(task.getName())
                    .summary("TASK_INCREMENT_STUCK")
                    .metric(AlarmKeyEnum.TASK_INCREMENT_STUCK)
                    .param(param)
                    .build();
            alarmInfo.setUserId(task.getUserId());
            alarmService.save(alarmInfo);
        } catch (Exception exception) {
            // Alarm support is provided by the enterprise build; never let it break the scan.
            log.warn("Failed to raise increment-stuck alarm for task {}", task.getId(), exception);
        }
    }

    private UserDetail loadUser(TaskDto task) {
        try {
            return StringUtils.isBlank(task.getUserId()) ? null : userService.loadUserById(MongoUtils.toObjectId(task.getUserId()));
        } catch (Exception exception) {
            log.warn("Unable to load owner for increment-stuck task {}", task.getId(), exception);
            return null;
        }
    }

    private void persistStates(ObjectId taskId, Map<String, Object> states, List<String> changedUnits) {
        if (taskId == null || changedUnits == null || changedUnits.isEmpty()) {
            return;
        }
        Update update = new Update();
        for (String unit : changedUnits) {
            // Write each touched unit at its own dotted path instead of overwriting the whole
            // attrs.incrementStuck map, so a concurrent writer that only changed a *different*
            // unit is not clobbered. $set on the unit path replaces that unit's sub-document
            // wholesale (all persisted unit fields are written back together).
            update.set("attrs." + INCREMENT_STUCK + "." + unit, states.get(unit));
        }
        taskService.update(Query.query(Criteria.where("_id").is(taskId)), update);
    }

    private boolean inGracePeriod(TaskDto task, long now) {
        Date startTime = task.getStartTime();
        return startTime != null && now - startTime.getTime() < graceMs();
    }

    /**
     * A unit is monitored while it is in CDC stage. {@code logCollector} (mining) tasks may not carry
     * {@code syncStage == "CDC"}, so they fall back to their streaming progress signal
     * ({@code sourceTime} / {@code eventTime}) — the same values {@code LogCollectorService.getAttrsValues}
     * exposes for a node-pair.
     */
    private boolean isCdcUnit(TaskDto task, Map<String, Object> unitProgress) {
        if (unitProgress.isEmpty()) {
            return false;
        }
        String syncStage = String.valueOf(unitProgress.get(FIELD_SYNC_STAGE));
        if (CDC.equalsIgnoreCase(syncStage)) {
            return true;
        }
        if (TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(task.getSyncType())) {
            return unitProgress.get(FIELD_SOURCE_TIME) != null || unitProgress.get(FIELD_EVENT_TIME) != null;
        }
        return false;
    }

    private String progressSignature(Map<String, Object> progress) {
        Map<String, Object> signature = new LinkedHashMap<>();
        signature.put(FIELD_SYNC_STAGE, progress.get(FIELD_SYNC_STAGE));
        signature.put(FIELD_STREAM_OFFSET, progress.get(FIELD_STREAM_OFFSET));
        signature.put(FIELD_OFFSET, progress.get(FIELD_OFFSET));
        // connHeartbeat / logCollector advance via event/source time rather than stream offset.
        signature.put(FIELD_EVENT_TIME, progress.get(FIELD_EVENT_TIME));
        signature.put(FIELD_SOURCE_TIME, progress.get(FIELD_SOURCE_TIME));
        return JsonUtil.toJsonUseJackson(signature);
    }

    /**
     * The engine stores each {@code attrs.syncProgress} value as a JSON string of the SyncProgress
     * object; tolerate an already-deserialized map too. A blank/unparseable value yields an empty map
     * (treated as "not in CDC stage").
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseProgress(Object value) {
        if (value instanceof Map) {
            return new LinkedHashMap<>((Map<String, Object>) value);
        }
        if (value instanceof String && StringUtils.isNotBlank((String) value)) {
            try {
                Map<String, Object> parsed = JsonUtil.parseJson((String) value, LinkedHashMap.class);
                return parsed == null ? new LinkedHashMap<>() : parsed;
            } catch (Exception exception) {
                log.debug("Unable to parse syncProgress entry, skip: {}", exception.getMessage());
            }
        }
        return new LinkedHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object value) {
        return value instanceof Map ? new LinkedHashMap<>((Map<String, Object>) value) : new LinkedHashMap<>();
    }

    private long asLong(Object value, long defaultValue) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return value == null ? defaultValue : Long.parseLong(String.valueOf(value));
        } catch (Exception ignored) {
            return defaultValue;
        }
    }

    private String formatDuration(long millis) {
        long totalSeconds = Math.max(0L, millis) / 1000L;
        long hours = totalSeconds / 3600L;
        long minutes = (totalSeconds % 3600L) / 60L;
        long seconds = totalSeconds % 60L;
        return String.format("%dh%dm%ds", hours, minutes, seconds);
    }

    private long timeoutMs() {
        return SettingsEnum.JOB_INCREMENT_STUCK_TIMEOUT.getLongValue(DEFAULT_TIMEOUT_MS);
    }

    private long cooldownMs() {
        long configuredCooldown = SettingsEnum.JOB_INCREMENT_STUCK_RESTART_COOLDOWN.getLongValue(DEFAULT_COOLDOWN_MS);
        return Math.max(DEFAULT_COOLDOWN_MS, configuredCooldown);
    }

    private long graceMs() {
        return SettingsEnum.JOB_INCREMENT_STUCK_START_GRACE.getLongValue(DEFAULT_GRACE_MS);
    }

    private int maxRestarts() {
        return SettingsEnum.JOB_INCREMENT_STUCK_MAX_RESTART.getIntValue(DEFAULT_MAX_RESTARTS);
    }
}
