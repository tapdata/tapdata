package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.heartbeat.HeartbeatRecoveryProtocol;
import com.tapdata.tm.commons.task.heartbeat.HeartbeatWatchdog;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** External checkpoint observer. Recovery always goes through the engine's stop/start lock. */
@Component
@Slf4j
public class TaskHeartbeatWatchdogSchedule {
    @Autowired private TaskService taskService;
    @Autowired private MonitoringLogsService monitoringLogsService;
    @Autowired private UserService userService;
    private final Map<String, Observation> observations = new ConcurrentHashMap<>();

    @Scheduled(fixedDelay = HeartbeatWatchdog.SCAN_MS)
    @SchedulerLock(name = "taskHeartbeatWatchdog", lockAtMostFor = "5m", lockAtLeastFor = "1s")
    public void scan() {
        List<TaskDto> tasks = taskService.findAll(HeartbeatRecoveryProtocol.observation(Query.query(Criteria.where("status").is(TaskDto.STATUS_RUNNING)
                .and("attrs." + HeartbeatWatchdog.CONFIG + ".enabled").is(true))));
        Set<String> live = new HashSet<>();
        for (TaskDto task : tasks) {
            String id = task.getId().toHexString();
            live.add(id);
            try { inspect(task, System.currentTimeMillis()); }
            catch (Exception e) { log.warn("Heartbeat fallback failed for task {}", id, e); }
        }
        observations.keySet().retainAll(live);
    }

    void inspect(TaskDto task, long now) {
        if (!HeartbeatWatchdog.enabled(task)) return;
        String generation = task.getAgentId() + ":" + task.getTaskRecordId() + ":" + task.getLastStartDate()
                + ":" + HeartbeatWatchdog.attr(task, HeartbeatWatchdog.HEALTH).get("runId");
        Observation observation = observations.compute(task.getId().toHexString(), (id, old) ->
                old != null && old.generation.equals(generation) ? old : new Observation(generation, now));
        Map<String, Object> progress = HeartbeatWatchdog.attr(task, "syncProgress");
        Set<String> invalidUnits = HeartbeatWatchdog.invalidUnits(task, progress);
        if (invalidUnits.isEmpty()) {
            observation.warnedInvalidUnits = null;
        } else {
            String invalid = invalidUnits.toString();
            if (!invalid.equals(observation.warnedInvalidUnits)) {
                observation.warnedInvalidUnits = invalid;
                log.warn("TaskHeartbeat taskId={} watchdog disabled for invalid syncProgress units={}", task.getId(), invalidUnits);
            }
        }
        Map<String, String> stuck = invalidUnits.isEmpty()
                ? observation.detector.inspect(task, progress, now)
                : Collections.emptyMap();
        Map<String, Object> previous = HeartbeatWatchdog.attr(task, HeartbeatWatchdog.RECOVERY);
        Map<String, Object> next = HeartbeatWatchdog.propose(task, stuck, "TM", now,
                observation.recoveryElapsed(previous, now),
                HeartbeatWatchdog.number(HeartbeatWatchdog.attr(task, HeartbeatWatchdog.HEALTH).get("lastPersistedAt"), 0));
        if (next == null) return;
        String message = "Heartbeat recovery transition: " + previous.get("state") + " -> " + next.get("state");
        next.put("updatedAt", now);
        if (taskService.update(HeartbeatRecoveryProtocol.compare(task, previous), HeartbeatRecoveryProtocol.write(next)).getModifiedCount() == 1) {
            log.warn("TaskHeartbeat taskId={} {}", task.getId(), message);
            try {
                monitoringLogsService.startTaskErrorLog(task, userService.loadUserById(MongoUtils.toObjectId(task.getUserId())), message, Level.WARN);
            } catch (Exception e) {
                log.warn("Unable to write heartbeat monitoring log for {}", task.getId(), e);
            }
        }
    }

    private static final class Observation {
        final String generation;
        final HeartbeatWatchdog.Detector detector;
        String warnedInvalidUnits;
        Object recoveryId;
        Object recoveryState;
        long recoveryObservedAt;
        Observation(String generation, long now) {
            this.generation = generation;
            detector = new HeartbeatWatchdog.Detector(now);
        }

        /** Uses only this TM process's observation clock; never subtracts an engine timestamp. */
        long recoveryElapsed(Map<String, Object> recovery, long now) {
            Object id = recovery.get("id");
            Object state = recovery.get("state");
            if (!Objects.equals(id, recoveryId) || !Objects.equals(state, recoveryState)) {
                recoveryId = id;
                recoveryState = state;
                recoveryObservedAt = now;
            }
            return Math.max(0L, now - recoveryObservedAt);
        }
    }
}
