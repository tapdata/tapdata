package io.tapdata.flow.engine.V2.monitor.heartbeat;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.heartbeat.HeartbeatWatchdog;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Updated by real reader/checkpoint paths, never by a synthetic liveness heartbeat. */
public final class HeartbeatProgressRegistry {
    private static final Map<String, State> STATES = new ConcurrentHashMap<>();
    private HeartbeatProgressRegistry() { }

    public static State open(TaskDto task) {
        if (!HeartbeatWatchdog.enabled(task)) return null;
        State state = new State(task);
        STATES.put(task.getId().toHexString(), state);
        return state;
    }

    public static State get(String taskId) { return STATES.get(taskId); }

    public static void close(TaskDto task, State state) {
        if (state != null) STATES.remove(task.getId().toHexString(), state);
    }

    public static void persisted(TaskDto task, Map<String, String> progress) {
        State state = current(task);
        if (state != null) {
            state.progress.putAll(progress);
            state.lastPersistedAt = System.currentTimeMillis();
        }
    }

    public static void sourceHeartbeat(TaskDto task, String sourceId) {
        State state = current(task);
        if (state != null && sourceId != null) state.sourceHeartbeats.put(sourceId, System.currentTimeMillis());
    }

    private static State current(TaskDto task) {
        if (task == null || task.getId() == null) return null;
        State state = STATES.get(task.getId().toHexString());
        return state != null && Objects.equals(state.taskRecordId, task.getTaskRecordId()) ? state : null;
    }

    public static final class State {
        public final String runId = UUID.randomUUID().toString();
        public final Object taskRecordId;
        public final Map<String, Object> progress = new ConcurrentHashMap<>();
        public final Map<String, Long> sourceHeartbeats = new ConcurrentHashMap<>();
        public final HeartbeatWatchdog.Detector detector = new HeartbeatWatchdog.Detector(monotonicMillis());
        public volatile long lastPersistedAt;
        private State(TaskDto task) {
            taskRecordId = task.getTaskRecordId();
            progress.putAll(HeartbeatWatchdog.attr(task, "syncProgress"));
        }
    }

    public static long monotonicMillis() { return System.nanoTime() / 1_000_000; }
}
