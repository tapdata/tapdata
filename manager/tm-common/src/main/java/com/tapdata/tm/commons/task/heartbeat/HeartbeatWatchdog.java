package com.tapdata.tm.commons.task.heartbeat;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.*;

/** Shared, opt-in contract: selected CDC edges MUST advance even without DML. */
public final class HeartbeatWatchdog {
    public static final String CONFIG = "heartbeatWatchdog";
    public static final String HEALTH = "heartbeatHealth";
    public static final String RECOVERY = "heartbeatRecovery";
    public static final String RECOVERY_PATH = "attrs." + RECOVERY;
    public static final long SCAN_MS = 15_000;
    public static final long RECOVERY_TIMEOUT_MS = 120_000;
    public static final long WINDOW_MS = 3_600_000;
    public static final int MAX_ATTEMPTS = 3;

    private HeartbeatWatchdog() { }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> map(Object value) {
        return value instanceof Map ? (Map<String, Object>) value : Collections.emptyMap();
    }

    public static Map<String, Object> attr(TaskDto task, String key) {
        return map(task.getAttrs() == null ? null : task.getAttrs().get(key));
    }

    public static long number(Object value, long fallback) {
        return value instanceof Number ? ((Number) value).longValue() : fallback;
    }

    public static boolean enabled(TaskDto task) {
        Map<String, Object> config = attr(task, CONFIG);
        return Boolean.TRUE.equals(config.get("enabled")) && !units(task).isEmpty()
                && timeout(task) >= 60_000 && grace(task) >= SCAN_MS
                && !task.isTestTask() && !task.isPreviewTask()
                && !Boolean.TRUE.equals(task.getShareCdcEnable())
                && !task.getAttrs().containsKey(TaskDto.ATTRS_USED_SHARE_CACHE)
                && (TaskDto.SYNC_TYPE_SYNC.equals(task.getSyncType())
                    || TaskDto.SYNC_TYPE_MIGRATE.equals(task.getSyncType()));
    }

    public static Set<String> units(TaskDto task) {
        Object value = attr(task, CONFIG).get("units");
        if (!(value instanceof Collection)) return Collections.emptySet();
        Set<String> units = new LinkedHashSet<>();
        for (Object unit : (Collection<?>) value) {
            if (unit instanceof String && !((String) unit).isEmpty()) units.add((String) unit);
        }
        return units;
    }

    public static long timeout(TaskDto task) {
        return number(attr(task, CONFIG).get("timeoutMs"), 180_000);
    }

    public static long grace(TaskDto task) {
        return number(attr(task, CONFIG).get("graceMs"), 180_000);
    }

    /** Hash only progress values, never the time at which a snapshot was re-uploaded. */
    public static String fingerprint(Object value) {
        try {
            if (!(value instanceof String) && !(value instanceof Map)) return null;
            Map<String, Object> progress = map(JsonUtil.parseJson(value instanceof String ? (String) value
                    : JsonUtil.toJsonUseJackson(value), LinkedHashMap.class));
            if (!"CDC".equals(progress.get("syncStage")) || progress.get("streamOffset") == null) return null;
            // A fresh timestamp with the old resume offset does not extend log retention safety.
            return DigestUtils.sha256Hex(JsonUtil.toJsonUseJackson(progress.get("streamOffset")));
        } catch (Exception ignored) {
            return null; // Unknown/initial-sync progress is not evidence of a CDC stall.
        }
    }

    public static Map<String, String> fingerprints(TaskDto task, Map<String, ?> progress) {
        Map<String, String> result = new LinkedHashMap<>();
        for (String unit : units(task)) {
            String fingerprint = fingerprint(progress.get(unit));
            if (fingerprint != null) result.put(unit, fingerprint);
        }
        return result;
    }

    /** Configured edge keys which cannot be found in an existing persisted progress map. */
    public static Set<String> invalidUnits(TaskDto task, Map<String, ?> progress) {
        if (progress == null || progress.isEmpty()) return Collections.emptySet();
        Set<String> invalid = new LinkedHashSet<>(units(task));
        invalid.removeAll(progress.keySet());
        return invalid;
    }

    public static boolean pending(Map<String, Object> recovery) {
        return Arrays.asList("REQUESTED", "STOPPING", "VERIFYING", "BLOCKED").contains(recovery.get("state"));
    }

    public static List<Long> recentAttempts(Map<String, Object> recovery, long now) {
        List<Long> result = new ArrayList<>();
        Object attempts = recovery.get("attempts");
        if (attempts instanceof Collection) {
            for (Object attempt : (Collection<?>) attempts) {
                long at = number(attempt, 0);
                if (at > now - WINDOW_MS) result.add(at);
            }
        }
        return result;
    }

    public static Map<String, Object> request(TaskDto task, Map<String, String> stuck, String origin, long now) {
        Map<String, Object> previous = attr(task, RECOVERY);
        if (pending(previous) || stuck.isEmpty()) return null;
        List<Long> attempts = recentAttempts(previous, now);
        if (attempts.size() >= MAX_ATTEMPTS) return null;
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("id", UUID.randomUUID().toString());
        request.put("state", "REQUESTED");
        request.put("requestedAt", now);
        request.put("origin", origin);
        request.put("fingerprints", stuck);
        request.put("attempts", attempts);
        return request;
    }

    public static boolean advanced(TaskDto task, Map<String, Object> recovery) {
        Map<String, Object> baseline = map(recovery.get("fingerprints"));
        Map<String, Object> progress = attr(task, "syncProgress");
        if (baseline.isEmpty()) return false;
        for (Map.Entry<String, Object> entry : baseline.entrySet()) {
            String current = fingerprint(progress.get(entry.getKey()));
            if (current == null || current.equals(entry.getValue())) return false;
        }
        return true;
    }

    /** Caller supplies monotonic elapsed time locally; TM uses its own observation clock. */
    public static final class Detector {
        private final Map<String, String> signatures = new HashMap<>();
        private final Map<String, Long> lastAdvance = new HashMap<>();
        private final long startedAt;

        public Detector(long now) { startedAt = now; }

        public Map<String, String> inspect(TaskDto task, Map<String, ?> progress, long now) {
            Map<String, String> current = fingerprints(task, progress);
            signatures.keySet().retainAll(current.keySet());
            lastAdvance.keySet().retainAll(current.keySet());
            Map<String, String> stuck = new LinkedHashMap<>();
            current.forEach((unit, signature) -> {
                if (!signature.equals(signatures.put(unit, signature))) lastAdvance.put(unit, now);
                else if (now - startedAt >= grace(task) && now - lastAdvance.get(unit) >= timeout(task)) {
                    stuck.put(unit, signature);
                }
            });
            return stuck;
        }
    }
}
