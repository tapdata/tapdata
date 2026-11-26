package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.AgentThrottleDeltaResponse;
import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import io.tapdata.observable.metric.throttle.MonotonicClock;
import io.tapdata.observable.metric.throttle.ThrottleAggregatorRegistry;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Process-level registry holding active {@link TaskThrottleRuntime} instances,
 * the shared {@link AgentMemoryBudgetPool}, pending policy apply receipts,
 * and pending graceful stop evidences awaiting control-plane acknowledgement.
 */
@Component
public class TaskThrottleRuntimeRegistry {

    private static final AtomicReference<TaskThrottleRuntimeRegistry> INSTANCE =
            new AtomicReference<>(new TaskThrottleRuntimeRegistry(new AgentMemoryBudgetPool(), MonotonicClock.SYSTEM));

    private final MonotonicClock clock;
    private final AgentMemoryBudgetPool agentMemoryPool;
    private final ConcurrentHashMap<String, TaskThrottleRuntime> activeRuntimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RunningTaskMeta> runningTasks = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<TaskThrottleApplyReceipt> pendingReceipts = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<TaskThrottleStopEvidenceDto> pendingStopEvidences = new ConcurrentLinkedQueue<>();

    private record RunningTaskMeta(String tenantId, String taskKind, String taskId) {
    }

    private volatile String agentId = "unknown-agent";
    private volatile long cdcSampleConfigVersion = 0L;
    private volatile int cdcSampleRefreshIntervalSeconds = ThrottleConstants.DEFAULT_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS;
    private volatile long lastDeltaCursor = 0L;

    public TaskThrottleRuntimeRegistry() {
        this(new AgentMemoryBudgetPool(), MonotonicClock.SYSTEM);
        INSTANCE.set(this);
    }

    public TaskThrottleRuntimeRegistry(AgentMemoryBudgetPool agentMemoryPool, MonotonicClock clock) {
        this.agentMemoryPool = agentMemoryPool == null ? new AgentMemoryBudgetPool() : agentMemoryPool;
        this.clock = clock == null ? MonotonicClock.SYSTEM : clock;
    }

    public static TaskThrottleRuntimeRegistry getInstance() {
        return INSTANCE.get();
    }

    public static void setInstanceForTest(TaskThrottleRuntimeRegistry custom) {
        INSTANCE.set(custom == null
                ? new TaskThrottleRuntimeRegistry(new AgentMemoryBudgetPool(), MonotonicClock.SYSTEM)
                : custom);
    }

    public void resetForTest() {
        for (TaskThrottleRuntime rt : activeRuntimes.values()) {
            rt.shutdownAndBuildStopEvidence(agentId, false, "test reset");
        }
        activeRuntimes.clear();
        runningTasks.clear();
        pendingReceipts.clear();
        pendingStopEvidences.clear();
        cdcSampleConfigVersion = 0L;
        cdcSampleRefreshIntervalSeconds = ThrottleConstants.DEFAULT_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS;
        lastDeltaCursor = 0L;
        ThrottleAggregatorRegistry.clearForTest();
    }

    public void registerRunningTask(String taskId, String tenantId, String taskKind) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        String normalizedTaskId = taskId.trim();
        runningTasks.put(normalizedTaskId, new RunningTaskMeta(
                tenantId != null ? tenantId : "",
                taskKind != null && !taskKind.isBlank() ? taskKind : "sync",
                normalizedTaskId
        ));
    }

    public void unregisterRunningTask(String taskId) {
        if (taskId != null && !taskId.isBlank()) {
            runningTasks.remove(taskId.trim());
        }
    }

    public AgentMemoryBudgetPool getAgentMemoryPool() {
        return agentMemoryPool;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        if (agentId != null && !agentId.isBlank()) {
            this.agentId = agentId.trim();
        }
    }

    public long getCdcSampleConfigVersion() {
        return cdcSampleConfigVersion;
    }

    public int getCdcSampleRefreshIntervalSeconds() {
        return cdcSampleRefreshIntervalSeconds;
    }

    public long getLastDeltaCursor() {
        return lastDeltaCursor;
    }

    /**
     * Opens or replaces the runtime for {@code runKey}. Any prior epoch runtime for the same taskId
     * is synchronously fenced and drained so old leases never leak across restarts.
     */
    public TaskThrottleRuntime openRuntime(TaskRunKey runKey, TaskThrottlePolicyDto initialPolicy, boolean protectedRun) {
        TaskThrottleRuntime created = new TaskThrottleRuntime(runKey, agentMemoryPool, clock, protectedRun);
        created.setAgentId(agentId);
        created.updateCdcSampleInterval(cdcSampleRefreshIntervalSeconds, cdcSampleConfigVersion);
        if (initialPolicy != null) {
            TaskThrottleApplyReceipt receipt = created.applyPolicy(initialPolicy, agentId, null);
            pendingReceipts.offer(receipt);
        }
        TaskThrottleRuntime previous = activeRuntimes.put(runKey.getTaskId(), created);
        ThrottleAggregatorRegistry.register(runKey.getTaskId(), created.getAggregator());
        if (previous != null && previous != created) {
            ThrottleAggregatorRegistry.unregister(runKey.getTaskId(), previous.getAggregator());
            previous.markFenced("Replaced by newer runEpoch " + runKey.getRunEpoch());
            previous.shutdownAndBuildStopEvidence(agentId, false, "Replaced locally by newer runEpoch");
        }
        return created;
    }

    public TaskThrottleRuntime getActive(String taskId) {
        if (taskId == null) {
            return null;
        }
        TaskThrottleRuntime rt = activeRuntimes.get(taskId);
        return (rt != null && !rt.isClosed()) ? rt : null;
    }

    public TaskThrottleStopEvidenceDto closeRuntime(String taskId, boolean writesSettled, String note) {
        if (taskId == null) {
            return null;
        }
        TaskThrottleRuntime removed = activeRuntimes.remove(taskId);
        if (removed == null) {
            return null;
        }
        ThrottleAggregatorRegistry.unregister(taskId, removed.getAggregator());
        TaskThrottleStopEvidenceDto evidence = removed.shutdownAndBuildStopEvidence(agentId, writesSettled, note);
        if (removed.isProtectedRun()) {
            pendingStopEvidences.offer(evidence);
        }
        return evidence;
    }

    /**
     * Applies an incremental delta from TM:
     * 1. Updates agent-wide CDC sample interval when config version advances.
     * 2. Fences stale local runEpochs or applies policy hot-updates cleanly with receipts.
     */
    public void applyDelta(AgentThrottleDeltaResponse delta) {
        if (delta == null) {
            return;
        }
        if (delta.getCursor() > this.lastDeltaCursor) {
            this.lastDeltaCursor = delta.getCursor();
        }
        if (delta.getSampleConfigVersion() > this.cdcSampleConfigVersion
                && delta.getCdcSampleRefreshIntervalSeconds() >= ThrottleConstants.MIN_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS
                && delta.getCdcSampleRefreshIntervalSeconds() <= ThrottleConstants.MAX_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS) {
            this.cdcSampleConfigVersion = delta.getSampleConfigVersion();
            this.cdcSampleRefreshIntervalSeconds = delta.getCdcSampleRefreshIntervalSeconds();
            for (TaskThrottleRuntime runtime : activeRuntimes.values()) {
                runtime.updateCdcSampleInterval(cdcSampleRefreshIntervalSeconds, cdcSampleConfigVersion);
            }
        }
        if (delta.getDirectives() == null || delta.getDirectives().isEmpty()) {
            return;
        }
        for (AgentThrottleDeltaResponse.TaskDirective directive : delta.getDirectives()) {
            if (directive == null || directive.getTaskId() == null) {
                continue;
            }
            TaskThrottleRuntime runtime = activeRuntimes.get(directive.getTaskId());
            if (runtime == null || runtime.isClosed()) {
                if (!directive.isFenced() && directive.getPolicy() != null) {
                    RunningTaskMeta meta = runningTasks.get(directive.getTaskId());
                    if (meta != null) {
                        String tenantId = directive.getTenantId() != null ? directive.getTenantId() : meta.tenantId();
                        String taskKind = directive.getTaskKind() != null ? directive.getTaskKind() : meta.taskKind();
                        openRuntime(
                                new TaskRunKey(tenantId, taskKind, directive.getTaskId(), Math.max(0L, directive.getAuthorizedRunEpoch())),
                                directive.getPolicy(),
                                directive.isProtectedRun()
                        );
                    }
                }
                continue;
            }
            if (directive.isFenced()
                    || (directive.getAuthorizedRunEpoch() > 0L
                    && directive.getAuthorizedRunEpoch() != runtime.getRunKey().getRunEpoch())) {
                runtime.markFenced("TM directive fenced local runEpoch " + runtime.getRunKey().getRunEpoch()
                        + " (authorizedRunEpoch=" + directive.getAuthorizedRunEpoch() + ")");
                continue;
            }
            if (directive.isProtectedRun()) {
                runtime.setProtectedRun(true);
            }
            if (directive.getPolicy() != null) {
                TaskThrottleApplyReceipt receipt = runtime.applyPolicy(directive.getPolicy(), agentId, null);
                pendingReceipts.offer(receipt);
            }
        }
    }

    public List<TaskThrottleApplyReceipt> drainPendingReceipts() {
        List<TaskThrottleApplyReceipt> list = new ArrayList<>();
        TaskThrottleApplyReceipt item;
        while ((item = pendingReceipts.poll()) != null) {
            list.add(item);
        }
        return list;
    }

    public List<TaskThrottleStopEvidenceDto> drainPendingStopEvidences() {
        List<TaskThrottleStopEvidenceDto> list = new ArrayList<>();
        TaskThrottleStopEvidenceDto item;
        while ((item = pendingStopEvidences.poll()) != null) {
            list.add(item);
        }
        return list;
    }

    public void requeueStopEvidence(TaskThrottleStopEvidenceDto evidence) {
        if (evidence == null) {
            return;
        }
        String ref = evidence.getEvidenceRef();
        for (TaskThrottleStopEvidenceDto existing : pendingStopEvidences) {
            if (existing == evidence || (ref != null && ref.equals(existing.getEvidenceRef()))) {
                return;
            }
        }
        pendingStopEvidences.offer(evidence);
    }

    public void removeStopEvidence(TaskThrottleStopEvidenceDto evidence) {
        if (evidence == null) {
            return;
        }
        pendingStopEvidences.remove(evidence);
        String ref = evidence.getEvidenceRef();
        if (ref != null) {
            pendingStopEvidences.removeIf(item -> ref.equals(item.getEvidenceRef()));
        }
    }

    public Map<String, List<TaskThrottleTelemetrySample>> snapshotTelemetryAll() {
        Map<String, List<TaskThrottleTelemetrySample>> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, TaskThrottleRuntime> entry : activeRuntimes.entrySet()) {
            TaskThrottleRuntime rt = entry.getValue();
            if (!rt.isClosed()) {
                List<TaskThrottleTelemetrySample> samples = rt.snapshotAndRollTelemetry();
                if (!samples.isEmpty()) {
                    result.put(entry.getKey(), samples);
                }
            }
        }
        return result;
    }

    public AgentThrottleCapabilities buildCapabilities() {
        return AgentThrottleCapabilities.builder()
                .schemaVersion(1)
                .agentId(agentId)
                .throttleSupported(true)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .supportedTaskKinds(Set.of("sync", "migrate", "logCollector"))
                .supportedPaths(Set.of("pdk-direct", "pdk-partition", "pdk-share-cdc"))
                .supportedDimensions(EnumSet.of(ThrottleDimension.BYTES, ThrottleDimension.QPS))
                .cdcSampleConfigVersion(cdcSampleConfigVersion)
                .cdcSampleRefreshIntervalSeconds(cdcSampleRefreshIntervalSeconds)
                .reportedAt(clock.currentTimeMillis())
                .build();
    }
}
