package io.tapdata.flow.engine.V2.monitor.heartbeat;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.heartbeat.HeartbeatRecoveryProtocol;
import com.tapdata.tm.commons.task.heartbeat.HeartbeatWatchdog;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.observable.logging.ObsLoggerFactory;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.*;
import java.util.concurrent.*;

/** Independent scanner; blocking stop operations never occupy the health-check thread. */
@Component
public class HeartbeatProgressWatchdog {
    private static final Logger LOG = LogManager.getLogger(HeartbeatProgressWatchdog.class);
    @Autowired private TapdataTaskScheduler scheduler;
    @Autowired @Qualifier("pingClientMongoOperator") private ClientMongoOperator mongo;
    private final ScheduledExecutorService scanner = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "Heartbeat-Watchdog"));
    private final ExecutorService recoveryWorkers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(16), r -> daemon(r, "Heartbeat-Recovery"));
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    private final Set<String> invalidUnitWarnings = ConcurrentHashMap.newKeySet();

    private static Thread daemon(Runnable runnable, String name) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        return thread;
    }

    @PostConstruct public void start() {
        scanner.scheduleWithFixedDelay(this::scan, HeartbeatWatchdog.SCAN_MS, HeartbeatWatchdog.SCAN_MS, TimeUnit.MILLISECONDS);
    }

    @PreDestroy public void close() {
        scanner.shutdownNow();
        recoveryWorkers.shutdownNow();
        invalidUnitWarnings.clear();
    }

    void scan() {
        for (TaskClient<TaskDto> client : scheduler.getTaskClientMap().values()) {
            try { inspect(client); }
            catch (Exception e) { LOG.warn("Heartbeat watchdog scan failed for {}", client.getTask().getId(), e); }
        }
    }

    void inspect(TaskClient<TaskDto> client) {
        String taskId = client.getTask().getId().toHexString();
        HeartbeatProgressRegistry.State local = HeartbeatProgressRegistry.get(taskId);
        if (local == null) return;
        TaskDto task = mongo.findOne(HeartbeatRecoveryProtocol.owner(client.getTask()), ConnectorConstant.TASK_COLLECTION, TaskDto.class);
        if (task == null || !HeartbeatWatchdog.enabled(task)) return;
        long now = System.currentTimeMillis();
        Set<String> invalidUnits = HeartbeatWatchdog.invalidUnits(task, local.progress);
        if (invalidUnits.isEmpty()) {
            invalidUnitWarnings.removeIf(key -> key.startsWith(taskId + ":"));
        } else {
            String warningKey = taskId + ":" + invalidUnits;
            if (invalidUnitWarnings.add(warningKey)) {
                warn(task, "Heartbeat watchdog disabled for invalid syncProgress units=" + invalidUnits);
            }
        }
        Map<String, String> stuck = invalidUnits.isEmpty()
                ? local.detector.inspect(task, local.progress, HeartbeatProgressRegistry.monotonicMillis())
                : Collections.emptyMap();
        Map<String, Object> health = new LinkedHashMap<>();
        health.put("runId", local.runId);
        health.put("reportedAt", now);
        health.put("lastPersistedAt", local.lastPersistedAt);
        health.put("sourceHeartbeats", new HashMap<>(local.sourceHeartbeats));
        health.put("state", invalidUnits.isEmpty() ? (stuck.isEmpty() ? "OBSERVING" : "STALLED") : "CONFIG_INVALID");
        health.put("stuckUnits", new ArrayList<>(stuck.keySet()));
        health.put("invalidUnits", new ArrayList<>(invalidUnits));
        mongo.update(HeartbeatRecoveryProtocol.owner(task), Update.update("attrs." + HeartbeatWatchdog.HEALTH, health), ConnectorConstant.TASK_COLLECTION);

        Map<String, Object> recovery = HeartbeatWatchdog.attr(task, HeartbeatWatchdog.RECOVERY);
        String state = String.valueOf(recovery.get("state"));
        if ("VERIFYING".equals(state)) {
            if (local.lastPersistedAt > HeartbeatWatchdog.number(recovery.get("startedAt"), now)
                    && HeartbeatWatchdog.advanced(task, recovery)) {
                if (transition(task, recovery, "RECOVERED", now)) warn(task, "Heartbeat recovery verified: persisted checkpoint advanced");
            }
            else if (now - HeartbeatWatchdog.number(recovery.get("startedAt"), now) > HeartbeatWatchdog.timeout(task) + HeartbeatWatchdog.grace(task)) {
                if (transition(task, recovery, "FAILED", now)) warn(task, "Restart completed but checkpoint progress did not recover");
            }
            return;
        }
        if ("BLOCKED".equals(state)) return;
        if ("CIRCUIT_OPEN".equals(state)) return;
        if ("REQUESTED".equals(state) && HeartbeatWatchdog.advanced(task, recovery)) {
            transition(task, recovery, "CANCELLED", now);
            return;
        }
        if ("STOPPING".equals(state) || "REQUESTED".equals(state)) {
            long since = HeartbeatWatchdog.number(recovery.get("claimedAt"), HeartbeatWatchdog.number(recovery.get("requestedAt"), now));
            if (now - since >= HeartbeatWatchdog.RECOVERY_TIMEOUT_MS) {
                if (transition(task, recovery, "BLOCKED", now)) warn(task, "Recovery timed out; old task termination unconfirmed, manual intervention required");
                return;
            }
            if ("STOPPING".equals(state)) return;
        }
        if (!"REQUESTED".equals(state)) {
            Map<String, Object> request = HeartbeatWatchdog.request(task, stuck, "ENGINE", now);
            if (request == null) {
                if (!stuck.isEmpty() && HeartbeatWatchdog.recentAttempts(recovery, now).size() >= HeartbeatWatchdog.MAX_ATTEMPTS
                        && transition(task, recovery, "CIRCUIT_OPEN", now)) {
                    warn(task, "Heartbeat recovery budget exhausted (3 attempts/hour); manual intervention required");
                }
                return;
            }
            if (!replace(task, recovery, request)) return;
            recovery = request;
            warn(task, "Heartbeat checkpoint stalled; recovery requested, units=" + stuck.keySet());
        }
        // TM can request recovery when this scanner was delayed. Discard obsolete evidence.
        if (HeartbeatWatchdog.advanced(task, recovery)) {
            transition(task, recovery, "CANCELLED", now);
            return;
        }
        final Map<String, Object> request = recovery;
        if (!inFlight.add(taskId)) return;
        try {
            recoveryWorkers.submit(() -> {
                try { recover(client, task, request, local); }
                finally { inFlight.remove(taskId); }
            });
        } catch (RejectedExecutionException e) {
            inFlight.remove(taskId); // Leave REQUESTED for the next scan/TM deadline alarm.
        }
    }

    void recover(TaskClient<TaskDto> client, TaskDto task, Map<String, Object> request, HeartbeatProgressRegistry.State local) {
        Map<String, Object> stopping = new LinkedHashMap<>(request);
        long now = System.currentTimeMillis();
        List<Long> attempts = HeartbeatWatchdog.recentAttempts(request, now);
        if (attempts.size() >= HeartbeatWatchdog.MAX_ATTEMPTS) {
            if (transition(task, request, "CIRCUIT_OPEN", now)) {
                warn(task, "Heartbeat recovery budget exhausted (3 attempts/hour); manual intervention required");
            }
            return;
        }
        attempts.add(now);
        stopping.put("attempts", attempts);
        stopping.put("state", "STOPPING");
        stopping.put("claimedAt", now);
        Map<String, Object> verifying = new LinkedHashMap<>(stopping);
        verifying.put("state", "VERIFYING");
        try {
            scheduler.recoverHeartbeatTask(client, () -> {
                TaskDto fresh = mongo.findOne(HeartbeatRecoveryProtocol.owner(task), ConnectorConstant.TASK_COLLECTION, TaskDto.class);
                if (fresh == null) return false;
                if (HeartbeatWatchdog.advanced(fresh, request)) {
                    transition(task, request, "CANCELLED", System.currentTimeMillis());
                    return false;
                }
                if (!replace(task, request, stopping)) return false;
                try { diagnostics(task, local); }
                catch (Exception e) { LOG.warn("Unable to collect heartbeat diagnostics for {}", task.getId(), e); }
                return true;
            }, () -> {
                // This CAS also rejects a timed-out recovery marked BLOCKED by TM.
                verifying.put("startedAt", System.currentTimeMillis());
                return replace(task, stopping, verifying);
            });
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            LOG.warn("Heartbeat recovery failed for {}", task.getId(), e);
            // A thrown stop may have left the old writer alive. Never release for another try.
            blockCurrentRecovery(task, request, System.currentTimeMillis());
            warn(task, "Heartbeat recovery could not confirm safe completion; manual intervention required");
        }
    }

    /** Reload the CAS document because startTask may already have moved STOPPING to VERIFYING. */
    private boolean blockCurrentRecovery(TaskDto task, Map<String, Object> request, long now) {
        TaskDto current = mongo.findOne(HeartbeatRecoveryProtocol.owner(task), ConnectorConstant.TASK_COLLECTION, TaskDto.class);
        if (current == null) return false;
        Map<String, Object> recovery = HeartbeatWatchdog.attr(current, HeartbeatWatchdog.RECOVERY);
        if (!Objects.equals(request.get("id"), recovery.get("id"))) return false;
        String state = String.valueOf(recovery.get("state"));
        if (!Arrays.asList("REQUESTED", "STOPPING", "VERIFYING").contains(state)) return false;
        return transition(current, recovery, "BLOCKED", now);
    }

    private boolean transition(TaskDto task, Map<String, Object> previous, String state, long now) {
        Map<String, Object> next = new LinkedHashMap<>(previous);
        next.put("state", state);
        next.put("updatedAt", now);
        return replace(task, previous, next);
    }

    private boolean replace(TaskDto task, Map<String, Object> previous, Map<String, Object> next) {
        return mongo.update(HeartbeatRecoveryProtocol.compare(task, previous), HeartbeatRecoveryProtocol.write(next),
                ConnectorConstant.TASK_COLLECTION).getModifiedCount() == 1;
    }

    private void diagnostics(TaskDto task, HeartbeatProgressRegistry.State local) {
        warn(task, "Stopping stalled task; lastPersistedAt=" + local.lastPersistedAt
                + ", sourceHeartbeats=" + local.sourceHeartbeats + ", runId=" + local.runId);
        // Bounded stacks, no offset payloads, SQL or connector credentials.
        int remaining = 8;
        for (ThreadInfo info : ManagementFactory.getThreadMXBean().getThreadInfo(ManagementFactory.getThreadMXBean().getAllThreadIds(), 12)) {
            if (info != null && info.getThreadName().contains(task.getId().toHexString())) {
                LOG.warn("Heartbeat recovery diagnostic: {}", info);
                if (--remaining == 0) break;
            }
        }
    }

    private void warn(TaskDto task, String message) {
        LOG.warn("TaskHeartbeat taskId={} {}", task.getId(), message);
        Optional.ofNullable(ObsLoggerFactory.getInstance().getObsLogger(task)).ifPresent(log -> log.warn(message));
    }
}
