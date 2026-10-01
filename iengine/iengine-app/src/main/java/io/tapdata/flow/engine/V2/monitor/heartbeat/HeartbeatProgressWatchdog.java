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
    // Tracks, per taskId, when THIS engine process first observed the current REQUESTED request id.
    // requestedAt may have been written by TM's clock; never subtract it from this engine's
    // System.currentTimeMillis() to decide a local timeout.
    private final Map<String, Object[]> requestFirstObservedAt = new ConcurrentHashMap<>();

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
        Set<String> live = new HashSet<>();
        for (TaskClient<TaskDto> client : scheduler.getTaskClientMap().values()) {
            String taskId = "unknown";
            try {
                taskId = client.getTask().getId().toHexString();
                live.add(taskId);
                inspect(client);
            } catch (Throwable e) {
                // Must never let an Error (or an exception thrown while building this very log
                // message) escape: scheduleWithFixedDelay silently cancels all future scans on any
                // uncaught Throwable, permanently killing this engine's watchdog until restart.
                LOG.warn("Heartbeat watchdog scan failed for {}", taskId, e);
            }
        }
        requestFirstObservedAt.keySet().retainAll(live);
    }

    /** Local-clock-only "first seen" timestamp for the current REQUESTED request id, analogous to
     *  TM's own Observation. Never derives elapsed time from a timestamp another process wrote. */
    private long requestObservedSince(String taskId, Object requestId, long now) {
        Object[] observed = requestFirstObservedAt.compute(taskId, (id, existing) ->
                existing != null && Objects.equals(existing[0], requestId) ? existing : new Object[]{requestId, now});
        return (long) observed[1];
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
        // Unlike BLOCKED, CIRCUIT_OPEN is not permanent: once the 1-hour attempt window has aged
        // out enough attempts, HeartbeatWatchdog.request() below allows a fresh request again (this
        // must match TM's TaskHeartbeatWatchdogSchedule.inspect(), which does the same). Do not
        // return early here or this engine would never re-request recovery after the window frees
        // up budget, even though TM would.
        if ("REQUESTED".equals(state) && HeartbeatWatchdog.advanced(task, recovery)) {
            transition(task, recovery, "CANCELLED", now);
            return;
        }
        if ("STOPPING".equals(state)) {
            // claimedAt is always written by the engine that claimed the request (see recover()),
            // so it is safe to compare against this engine's own clock.
            long since = HeartbeatWatchdog.number(recovery.get("claimedAt"), now);
            if (now - since >= HeartbeatWatchdog.RECOVERY_TIMEOUT_MS) {
                if (transition(task, recovery, "BLOCKED", now)) warn(task, "Recovery timed out; old task termination unconfirmed, manual intervention required");
            }
            return;
        }
        if ("REQUESTED".equals(state)) {
            long since = requestObservedSince(taskId, recovery.get("id"), now);
            if (now - since >= HeartbeatWatchdog.RECOVERY_TIMEOUT_MS) {
                // Nobody has claimed this request yet, so nothing has been stopped. BLOCKED is
                // permanent and would suppress ordinary error retries forever; CANCELLED is safely
                // retryable on the next stall detection.
                if (transition(task, recovery, "CANCELLED", now)) warn(task, "Unclaimed heartbeat recovery request timed out; will retry on next stall detection");
                return;
            }
        }
        if (!"REQUESTED".equals(state)) {
            Map<String, Object> request = HeartbeatWatchdog.request(task, stuck, "ENGINE", now);
            if (request == null) {
                // Already CIRCUIT_OPEN and still within the attempt window: nothing changed, so skip
                // re-writing/re-warning every scan. Only transition into CIRCUIT_OPEN the first time
                // the budget is exhausted.
                if (!"CIRCUIT_OPEN".equals(state) && !stuck.isEmpty()
                        && HeartbeatWatchdog.recentAttempts(recovery, now).size() >= HeartbeatWatchdog.MAX_ATTEMPTS
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
            boolean started = scheduler.recoverHeartbeatTask(client, () -> {
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
            if (!started) failFastIfVerifyingWithoutOwner(client, task, request);
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            LOG.warn("Heartbeat recovery failed for {}", task.getId(), e);
            // A thrown stop may have left the old writer alive. Never release for another try.
            blockCurrentRecovery(task, request, System.currentTimeMillis());
            warn(task, "Heartbeat recovery could not confirm safe completion; manual intervention required");
        }
    }

    /**
     * recoverHeartbeatTask() returned false without throwing. Most causes (lost CAS, obsolete
     * request, a live owner mismatch) already transitioned the recovery document themselves. The
     * one case that does NOT is: the STOPPING->VERIFYING CAS succeeded but startTask() afterwards
     * silently failed to attach a client (e.g. TmUnavailableException is only logged, or cache
     * cleanup deferred it) — see HeartbeatProgressWatchdog PR review. Left alone, that stalls in
     * VERIFYING with no owner anywhere until TM's timeout+grace (default 6 min) elapses. Detect it
     * here and fail fast so a fresh stall detection can request recovery again much sooner.
     */
    private void failFastIfVerifyingWithoutOwner(TaskClient<TaskDto> client, TaskDto task, Map<String, Object> request) {
        String taskId = task.getId().toHexString();
        if (scheduler.getTaskClientMap().containsKey(taskId)) return;
        TaskDto current = mongo.findOne(HeartbeatRecoveryProtocol.owner(task), ConnectorConstant.TASK_COLLECTION, TaskDto.class);
        if (current == null) return;
        Map<String, Object> recovery = HeartbeatWatchdog.attr(current, HeartbeatWatchdog.RECOVERY);
        if (!Objects.equals(request.get("id"), recovery.get("id")) || !"VERIFYING".equals(recovery.get("state"))) return;
        if (transition(current, recovery, "FAILED", System.currentTimeMillis())) {
            warn(task, "Heartbeat recovery restart did not attach a task client; marking recovery failed early");
        }
    }

    /** Reload the CAS document because startTask may already have moved STOPPING to VERIFYING. */
    private boolean blockCurrentRecovery(TaskDto task, Map<String, Object> request, long now) {
        TaskDto current = mongo.findOne(HeartbeatRecoveryProtocol.owner(task), ConnectorConstant.TASK_COLLECTION, TaskDto.class);
        if (current == null) return false;
        Map<String, Object> recovery = HeartbeatWatchdog.attr(current, HeartbeatWatchdog.RECOVERY);
        if (!Objects.equals(request.get("id"), recovery.get("id"))) return false;
        String state = String.valueOf(recovery.get("state"));
        // REQUESTED means the claim itself never confirmed a stop was even attempted (e.g. a
        // transient TM HTTP failure while reading before claiming). There is nothing to fence in
        // that case, so leave it REQUESTED for the next scan to retry instead of permanently
        // BLOCKing a task that was never touched.
        if (!Arrays.asList("STOPPING", "VERIFYING").contains(state)) return false;
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
        // Source/Target node processing threads are named after the DAG node id, not the task id
        // (e.g. "Target-Process-<node>[<nodeId>]"), so match on both. Otherwise a target hung in a
        // blocking write leaves no useful evidence for root-causing the stall.
        Set<String> markers = new HashSet<>();
        markers.add(task.getId().toHexString());
        if (task.getDag() != null && task.getDag().getNodes() != null) {
            task.getDag().getNodes().forEach(node -> {
                if (node != null && node.getId() != null) markers.add(node.getId());
            });
        }
        // Bounded stacks, no offset payloads, SQL or connector credentials.
        int remaining = 8;
        for (ThreadInfo info : ManagementFactory.getThreadMXBean().getThreadInfo(ManagementFactory.getThreadMXBean().getAllThreadIds(), 12)) {
            if (info != null && markers.stream().anyMatch(info.getThreadName()::contains)) {
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
