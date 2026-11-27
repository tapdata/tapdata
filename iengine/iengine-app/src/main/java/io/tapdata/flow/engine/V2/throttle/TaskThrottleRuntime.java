package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.WaitReason;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.observable.metric.throttle.BatchMeteringResult;
import io.tapdata.observable.metric.throttle.CdcTableDmlSampleCache;
import io.tapdata.observable.metric.throttle.ContinuousBatchMeter;
import io.tapdata.observable.metric.throttle.EventCostMeter;
import io.tapdata.observable.metric.throttle.MonotonicClock;
import io.tapdata.observable.metric.throttle.ThrottleMetricsWindowAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Per-task execution-epoch throttling and metering runtime unifying:
 * - continuous FULL/CDC metering on input and output directions,
 * - dual-dimension token bucket rate limiting,
 * - two-level in-flight capacity leases & 80%/60% write backpressure,
 * - CDC log retention monitoring & stale-epoch fencing / checkpoint isolation.
 */
public class TaskThrottleRuntime {

    private static final Logger logger = LoggerFactory.getLogger(TaskThrottleRuntime.class);

    public static final String INFO_KEY_LEASE_ID = "_throttleLeaseId";
    public static final String INFO_KEY_SOURCE_NODE_ID = "_throttleSourceNodeId";
    public static final String INFO_KEY_BATCH_SEQ = "_throttleBatchSeq";

    private static final long MIN_FALLBACK_RECORD_MEM_BYTES = 256L;
    private static final long DEFAULT_TASK_BUDGET_BYTES = 64L * 1024L * 1024L;
    private static final long DEFAULT_TASK_BUDGET_RECORDS = 200_000L;
    private static final long DEFAULT_TASK_BUDGET_BATCHES = 8_192L;
    private static final long DEFAULT_DRAIN_RESERVE_BYTES = 2L * 1024L * 1024L;
    private static final int MAX_SAMPLE_UNKNOWN_RETRIES = 8;
    private static final long SAMPLE_UNKNOWN_RETRY_SLEEP_MS = 50L;

    private final TaskRunKey runKey;
    private final MonotonicClock clock;
    private final EventCostMeter costMeter;
    private final CdcTableDmlSampleCache inputSampleCache;
    private final CdcTableDmlSampleCache outputSampleCache;
    private final ContinuousBatchMeter inputMeter;
    private final ContinuousBatchMeter outputMeter;
    private final ThrottleMetricsWindowAggregator aggregator;
    private final TaskThrottleController controller;
    private final InFlightBudget inFlightBudget;
    private final WriteBackpressureSignal backpressureSignal;
    private final LogRetentionMonitor logRetentionMonitor;

    private final AtomicLong batchSeqGenerator = new AtomicLong(1L);
    private final ConcurrentHashMap<String, Set<String>> sourceToTargets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> maxCompletedSeqBySourceAndTarget = new ConcurrentHashMap<>();

    private volatile boolean protectedRun;
    private volatile boolean fenced;
    private volatile String fenceReason;
    private volatile boolean closed;
    private volatile String agentId;

    public TaskThrottleRuntime(
            TaskRunKey runKey,
            AgentMemoryBudgetPool agentPool,
            MonotonicClock clock,
            boolean protectedRun) {
        this(
                runKey,
                agentPool,
                clock,
                protectedRun,
                DEFAULT_TASK_BUDGET_BYTES,
                DEFAULT_TASK_BUDGET_RECORDS,
                DEFAULT_TASK_BUDGET_BATCHES,
                DEFAULT_DRAIN_RESERVE_BYTES
        );
    }

    public TaskThrottleRuntime(
            TaskRunKey runKey,
            AgentMemoryBudgetPool agentPool,
            MonotonicClock clock,
            boolean protectedRun,
            long taskMaxBytes,
            long taskMaxRecords,
            long taskMaxBatches,
            long taskDrainReserveBytes) {
        if (runKey == null || agentPool == null) {
            throw new IllegalArgumentException("runKey and agentPool must not be null");
        }
        this.runKey = runKey;
        this.clock = clock == null ? MonotonicClock.SYSTEM : clock;
        this.protectedRun = protectedRun;

        this.costMeter = new EventCostMeter();
        this.inputSampleCache = new CdcTableDmlSampleCache(
                runKey.getTaskId(),
                runKey.getRunEpoch(),
                ThrottleDirection.INPUT,
                this.clock,
                costMeter,
                CdcTableDmlSampleCache.CacheConfig.defaults());
        this.outputSampleCache = new CdcTableDmlSampleCache(
                runKey.getTaskId(),
                runKey.getRunEpoch(),
                ThrottleDirection.OUTPUT,
                this.clock,
                costMeter,
                CdcTableDmlSampleCache.CacheConfig.defaults());
        this.inputMeter = new ContinuousBatchMeter(runKey.getTaskId(), runKey.getRunEpoch(), ThrottleDirection.INPUT, costMeter, inputSampleCache);
        this.outputMeter = new ContinuousBatchMeter(runKey.getTaskId(), runKey.getRunEpoch(), ThrottleDirection.OUTPUT, costMeter, outputSampleCache);
        this.aggregator = new ThrottleMetricsWindowAggregator(runKey.getRunEpoch(), this.clock);
        this.controller = new TaskThrottleController(runKey, this.clock);
        this.inFlightBudget = new InFlightBudget(
                runKey,
                agentPool,
                taskMaxBytes,
                taskMaxRecords,
                taskMaxBatches,
                taskDrainReserveBytes
        );
        this.backpressureSignal = new WriteBackpressureSignal(this.inFlightBudget, this.clock);
        this.logRetentionMonitor = new LogRetentionMonitor();
    }

    public static final class ThrottleRunFencedException extends RuntimeException {
        public ThrottleRunFencedException(String message) {
            super(message);
        }
    }

    public static final class SliceDispatchCancelledException extends RuntimeException {
        public SliceDispatchCancelledException(String message) {
            super(message);
        }
    }

    public ThrottleRunFencedException buildFencedException(String stageDesc) {
        String prefix = (stageDesc == null || stageDesc.isBlank()) ? "Throttle runtime" : stageDesc;
        String reasonSuffix = (fenceReason != null && !fenceReason.isBlank()) ? " (" + fenceReason + ")" : "";
        return new ThrottleRunFencedException(
                prefix + " aborted because throttle runEpoch is fenced: " + runKey + reasonSuffix);
    }

    public void clearTargetWaiting(String targetNodeId, ThrottlePhase phase) {
        backpressureSignal.clearTargetWaiting(targetNodeId);
        if (!backpressureSignal.getWaitingTargetIds().isEmpty()) {
            return;
        }
        ThrottlePhase resolved = phase == null ? ThrottlePhase.FULL : phase;
        aggregator.updateWaitState(
                resolved,
                ThrottleDirection.OUTPUT,
                WaitReason.NONE,
                EnumSet.noneOf(ThrottleDimension.class),
                inFlightBudget.currentWatermarkRatio(),
                logRetentionMonitor.worstStatus()
        );
    }

    public TaskRunKey getRunKey() {
        return runKey;
    }

    public boolean isProtectedRun() {
        return protectedRun;
    }

    public void setProtectedRun(boolean protectedRun) {
        this.protectedRun = protectedRun;
    }

    public boolean isFenced() {
        return fenced;
    }

    public String getFenceReason() {
        return fenceReason;
    }

    public boolean isClosed() {
        return closed;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public TaskThrottleController getController() {
        return controller;
    }

    public InFlightBudget getInFlightBudget() {
        return inFlightBudget;
    }

    public WriteBackpressureSignal getBackpressureSignal() {
        return backpressureSignal;
    }

    public LogRetentionMonitor getLogRetentionMonitor() {
        return logRetentionMonitor;
    }

    public ThrottleMetricsWindowAggregator getAggregator() {
        return aggregator;
    }

    public ContinuousBatchMeter getInputMeter() {
        return inputMeter;
    }

    public ContinuousBatchMeter getOutputMeter() {
        return outputMeter;
    }

    /**
     * Marks this runtime as fenced (e.g., TM reassigned runEpoch elsewhere). Stops all new admissions
     * and blocks further checkpoint writes from this stale runtime.
     */
    public void markFenced(String reason) {
        this.fenced = true;
        this.fenceReason = reason == null ? "Stale runEpoch fenced by TM" : reason;
        controller.shutdown();
        inFlightBudget.cancelWaiting();
        backpressureSignal.signalDrainProgress();
        logger.warn("TaskThrottleRuntime fenced: runKey={}, reason={}", runKey, this.fenceReason);
    }

    public boolean beforeCheckpointPersist() {
        if (fenced) {
            logger.warn("Blocked stale checkpoint persistence for fenced runtime {}", runKey);
            return false;
        }
        return true;
    }

    /**
     * Appends {@code runEpoch} and {@code agentId} query params only for protected runs,
     * preserving unmodified collection URIs for non-protected legacy tests and flows.
     */
    public String buildSyncProgressResource(String baseCollection, String currentAgentId) {
        if (!protectedRun || runKey.getRunEpoch() <= 0L || baseCollection == null) {
            return baseCollection;
        }
        String sep = baseCollection.contains("?") ? "&" : "?";
        StringBuilder sb = new StringBuilder(baseCollection)
                .append(sep)
                .append("runEpoch=")
                .append(runKey.getRunEpoch());
        String resolvedAgent = currentAgentId != null && !currentAgentId.isBlank() ? currentAgentId : agentId;
        if (resolvedAgent != null && !resolvedAgent.isBlank()) {
            sb.append("&agentId=").append(URLEncoder.encode(resolvedAgent, StandardCharsets.UTF_8));
        }
        return sb.toString();
    }

    public TaskThrottleApplyReceipt applyPolicy(TaskThrottlePolicyDto dto, String currentAgentId, List<Long> skippedVersions) {
        if (currentAgentId != null && !currentAgentId.isBlank()) {
            this.agentId = currentAgentId;
        }
        TaskThrottleApplyReceipt receipt = controller.applyPolicy(dto, this.agentId, skippedVersions);
        if (runKey.getRunEpoch() > 0L
                && dto != null && dto.getPhases() != null
                && dto.getPhases().values().stream().anyMatch(p -> p != null && p.isEnabled())) {
            this.protectedRun = true;
        }
        return receipt;
    }

    /**
     * Registers source -> reachable target node dependencies so fan-out leases and
     * write backpressure track each branch independently.
     */
    public void registerTopology(Map<String, Set<String>> sourceToTargetsMap) {
        if (sourceToTargetsMap == null || sourceToTargetsMap.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Set<String>> entry : sourceToTargetsMap.entrySet()) {
            String sourceNodeId = entry.getKey();
            Set<String> targets = entry.getValue();
            if (sourceNodeId == null || sourceNodeId.isBlank() || targets == null || targets.isEmpty()) {
                continue;
            }
            Set<String> copy = Collections.unmodifiableSet(new HashSet<>(targets));
            this.sourceToTargets.put(sourceNodeId, copy);
            for (String targetNodeId : copy) {
                if (targetNodeId != null && !targetNodeId.isBlank()) {
                    backpressureSignal.registerDependency(sourceNodeId, targetNodeId);
                }
            }
        }
    }

    public void updateCdcSampleInterval(int intervalSeconds, long configVersion) {
        inputSampleCache.updateRefreshInterval(intervalSeconds, configVersion);
        outputSampleCache.updateRefreshInterval(intervalSeconds, configVersion);
    }

    /**
     * Source admission entrypoint called OUTSIDE sourceRunnerLock / critical connector locks.
     * Always meters batches continuously; applies backpressure, capacity lease reservation,
     * and (when phase throttling is enabled) token bucket slicing/gating before dispatching each slice.
     */
    public <T extends TapEvent> AdmitOutcome admitSourceBatch(
            String sourceNodeId,
            String endpointId,
            ThrottlePhase phase,
            List<T> events,
            Function<TapEvent, String> tableNormalizer,
            BooleanSupplier cancelled,
            Consumer<AdmittedSourceSlice<T>> sliceDispatcher) {
        if (events == null || events.isEmpty()) {
            return AdmitOutcome.ADMITTED;
        }
        if (fenced || closed) {
            return AdmitOutcome.FENCED_OR_STOPPED;
        }
        if (cancelled != null && cancelled.getAsBoolean()) {
            return AdmitOutcome.CANCELLED;
        }

        ThrottlePhase resolvedPhase = phase == null ? ThrottlePhase.FULL : phase;
        boolean phaseEnabled = controller.isPhaseEnabled(resolvedPhase);
        aggregator.activateSlot(resolvedPhase, ThrottleDirection.INPUT, controller.getAppliedVersion());

        long maxLeaseBytes = inFlightBudget.maxSingleLeaseBytes();
        long budgetMaxBytes = Math.max(1L, maxLeaseBytes / 2L);
        long budgetMaxRecords = Math.min(inFlightBudget.maxSingleLeaseRecords(),
                Math.max(1L, maxLeaseBytes / MIN_FALLBACK_RECORD_MEM_BYTES));

        if (!phaseEnabled) {
            BatchMeteringResult probe = meterBatch(inputMeter, resolvedPhase, endpointId, events, tableNormalizer, false);
            long fullMemBytes = estimateConservativeMemoryBytes(probe, events.size());
            if (events.size() <= 1 || inFlightBudget.canEverFit(fullMemBytes, events.size(), 1L)) {
                return admitSingleSlice(sourceNodeId, endpointId, resolvedPhase, events, tableNormalizer, probe, true, cancelled, sliceDispatcher);
            }
            int estimatedSliceSize = computeSliceRecordCount(events.size(), probe, budgetMaxRecords, budgetMaxBytes);
            int offset = 0;
            while (offset < events.size()) {
                if (fenced || closed) {
                    return AdmitOutcome.FENCED_OR_STOPPED;
                }
                if (cancelled != null && cancelled.getAsBoolean()) {
                    return AdmitOutcome.CANCELLED;
                }
                SubSlice<T> next = nextBoundedSlice(
                        events, offset, estimatedSliceSize, inputMeter, resolvedPhase, endpointId,
                        tableNormalizer, budgetMaxRecords, budgetMaxBytes, probe, false, true);
                AdmitOutcome outcome = admitSingleSlice(
                        sourceNodeId, endpointId, resolvedPhase, next.events(), tableNormalizer, next.meter(),
                        next.events().size() > 1, cancelled, sliceDispatcher);
                if (outcome != AdmitOutcome.ADMITTED) {
                    return outcome;
                }
                offset = next.nextOffset();
            }
            return AdmitOutcome.ADMITTED;
        }

        // Throttling enabled: slice large splittable batches so each piece stays within QPS/BYTES burst limits and hard capacity bounds.
        long chunkMaxEvents = Math.min(controller.maxSplittableEvents(resolvedPhase), budgetMaxRecords);
        long chunkMaxBytes = Math.min(controller.maxSplittableBytes(resolvedPhase), budgetMaxBytes);

        BatchMeteringResult probe = meterBatch(inputMeter, resolvedPhase, endpointId, events, tableNormalizer, true);
        int estimatedSliceSize = computeSliceRecordCount(events.size(), probe, chunkMaxEvents, chunkMaxBytes);

        int offset = 0;
        while (offset < events.size()) {
            if (fenced || closed) {
                return AdmitOutcome.FENCED_OR_STOPPED;
            }
            if (cancelled != null && cancelled.getAsBoolean()) {
                return AdmitOutcome.CANCELLED;
            }
            SubSlice<T> next = nextBoundedSlice(
                    events, offset, estimatedSliceSize, inputMeter, resolvedPhase, endpointId,
                    tableNormalizer, chunkMaxEvents, chunkMaxBytes, probe, true, true);
            boolean splittable = next.events().size() > 1;
            AdmitOutcome outcome = admitSingleSlice(
                    sourceNodeId, endpointId, resolvedPhase, next.events(), tableNormalizer, next.meter(), splittable, cancelled, sliceDispatcher);
            if (outcome != AdmitOutcome.ADMITTED) {
                return outcome;
            }
            offset = next.nextOffset();
        }
        return AdmitOutcome.ADMITTED;
    }

    private <T extends TapEvent> AdmitOutcome admitSingleSlice(
            String sourceNodeId,
            String endpointId,
            ThrottlePhase phase,
            List<T> sliceEvents,
            Function<TapEvent, String> tableNormalizer,
            BatchMeteringResult meterResult,
            boolean splittable,
            BooleanSupplier cancelled,
            Consumer<AdmittedSourceSlice<T>> sliceDispatcher) {
        long seq = batchSeqGenerator.getAndIncrement();
        long memBytes = estimateConservativeMemoryBytes(meterResult, sliceEvents.size());
        long recordCount = sliceEvents.size();

        long effectiveMemBytes = memBytes;
        long effectiveRecordCount = recordCount;
        if (!inFlightBudget.canEverFit(memBytes, recordCount, 1L)) {
            effectiveMemBytes = Math.min(memBytes, inFlightBudget.maxSingleLeaseBytes());
            effectiveRecordCount = Math.min(recordCount, inFlightBudget.maxSingleLeaseRecords());
            logger.warn("Single slice exceeds hard in-flight capacity budget (task={}, bytes={}, records={}); clamping lease to ({}, {}) for bounded-wait admission",
                    runKey, memBytes, recordCount, effectiveMemBytes, effectiveRecordCount);
        }

        CapacityLease lease = acquireCapacityWithBackpressure(sourceNodeId, seq, phase, effectiveMemBytes, effectiveRecordCount, cancelled);
        if (lease == null) {
            return (fenced || closed) ? AdmitOutcome.FENCED_OR_STOPPED : AdmitOutcome.CANCELLED;
        }

        boolean leaseTransferredToDispatcher = false;
        long appliedVersion = controller.getAppliedVersion();
        BatchMeteringResult effectiveMeter = meterResult;
        boolean tokenDebited = false;
        try {
            if (controller.isPhaseEnabled(phase)) {
                TaskThrottleController.WaitStateObserver observer = new TaskThrottleController.WaitStateObserver() {
                    @Override
                    public void onWaiting(TaskThrottleController.AcquireResult waitResult) {
                        WaitReason reason = waitResult.outcome() == TaskThrottleController.Outcome.SAMPLE_UNKNOWN_WAIT
                                ? WaitReason.SAMPLE_WAIT
                                : WaitReason.RATE_WAIT;
                        aggregator.updateWaitState(
                                phase,
                                ThrottleDirection.INPUT,
                                reason,
                                waitResult.waitingDimensions(),
                                inFlightBudget.currentWatermarkRatio(),
                                logRetentionMonitor.worstStatus()
                        );
                    }

                    @Override
                    public void onAdmitted(TaskThrottleController.AcquireResult admittedResult) {
                        aggregator.updateWaitState(
                                phase,
                                ThrottleDirection.INPUT,
                                WaitReason.NONE,
                                EnumSet.noneOf(ThrottleDimension.class),
                                inFlightBudget.currentWatermarkRatio(),
                                logRetentionMonitor.worstStatus()
                        );
                    }
                };
                MeteredAcquireResult acqResult = acquireQuotaWithSampleRetry(
                        phase, ThrottleDirection.INPUT, splittable, effectiveMeter,
                        () -> meterBatch(inputMeter, phase, endpointId, sliceEvents, tableNormalizer, true),
                        cancelled, observer);
                effectiveMeter = acqResult.meter();
                TaskThrottleController.AcquireResult acq = acqResult.acquire();

                appliedVersion = acq.appliedVersion();
                if (acq.outcome() == TaskThrottleController.Outcome.CANCELLED) {
                    return AdmitOutcome.CANCELLED;
                }
                if (acq.outcome() == TaskThrottleController.Outcome.STOPPED) {
                    return AdmitOutcome.FENCED_OR_STOPPED;
                }
                if (acq.outcome() == TaskThrottleController.Outcome.EXCEEDS_ATOMIC_LIMIT) {
                    throw new IllegalStateException(
                            "Atomic batch exceeds 5s throttle limit on " + acq.exceededDimension()
                                    + ": cost=" + acq.requestedCost() + ", limit=" + acq.limitCost());
                }
                tokenDebited = true;
            }

            if (fenced || closed || (cancelled != null && cancelled.getAsBoolean())) {
                if (tokenDebited) {
                    aggregator.recordCancelledAfterDebit(
                            phase, ThrottleDirection.INPUT, effectiveMeter.getDmlCount(), effectiveMeter.getEstimatedBytes());
                }
                return (fenced || closed) ? AdmitOutcome.FENCED_OR_STOPPED : AdmitOutcome.CANCELLED;
            }

            if (sliceDispatcher != null) {
                lease.transferOwnership(lease.getHolderVersion(), "pipeline-queue");
                try {
                    sliceDispatcher.accept(new AdmittedSourceSlice<>(sliceEvents, lease, sourceNodeId, seq));
                } catch (SliceDispatchCancelledException cancelledEx) {
                    if (tokenDebited) {
                        aggregator.recordCancelledAfterDebit(
                                phase, ThrottleDirection.INPUT, effectiveMeter.getDmlCount(), effectiveMeter.getEstimatedBytes());
                    }
                    return (fenced || closed) ? AdmitOutcome.FENCED_OR_STOPPED : AdmitOutcome.CANCELLED;
                }
                leaseTransferredToDispatcher = true;
            }
            aggregator.recordAdmitted(effectiveMeter, appliedVersion);
            return AdmitOutcome.ADMITTED;
        } catch (RuntimeException ex) {
            if (tokenDebited && !leaseTransferredToDispatcher) {
                aggregator.recordCancelledAfterDebit(
                        phase, ThrottleDirection.INPUT, effectiveMeter.getDmlCount(), effectiveMeter.getEstimatedBytes());
            }
            throw ex;
        } finally {
            if (!leaseTransferredToDispatcher) {
                lease.release(lease.getHolderVersion());
            }
        }
    }

    private CapacityLease acquireCapacityWithBackpressure(
            String sourceNodeId,
            long batchSeq,
            ThrottlePhase phase,
            long memBytes,
            long recordCount,
            BooleanSupplier cancelled) {
        while (true) {
            if (fenced || closed || (cancelled != null && cancelled.getAsBoolean())) {
                inFlightBudget.cancelWaiting();
                return null;
            }
            if (backpressureSignal.shouldPauseSourcePull(sourceNodeId)) {
                WaitReason reason = backpressureSignal.dominantUpstreamReason(sourceNodeId);
                aggregator.updateWaitState(
                        phase,
                        ThrottleDirection.INPUT,
                        reason == WaitReason.NONE ? WaitReason.BACKPRESSURE_WAIT : reason,
                        EnumSet.noneOf(ThrottleDimension.class),
                        inFlightBudget.currentWatermarkRatio(),
                        logRetentionMonitor.worstStatus()
                );
                backpressureSignal.awaitDrainStep(100L);
                continue;
            }
            CapacityLease lease = inFlightBudget.tryAcquireLease(
                    memBytes, recordCount, 1L, sourceNodeId, sourceNodeId, batchSeq,
                    sourceToTargets.getOrDefault(sourceNodeId, Collections.emptySet()), true);
            if (lease != null) {
                aggregator.updateWaitState(
                        phase,
                        ThrottleDirection.INPUT,
                        WaitReason.NONE,
                        EnumSet.noneOf(ThrottleDimension.class),
                        inFlightBudget.currentWatermarkRatio(),
                        logRetentionMonitor.worstStatus()
                );
                return lease;
            }
            aggregator.updateWaitState(
                    phase,
                    ThrottleDirection.INPUT,
                    WaitReason.BUFFER_WAIT,
                    EnumSet.noneOf(ThrottleDimension.class),
                    inFlightBudget.currentWatermarkRatio(),
                    logRetentionMonitor.worstStatus()
            );
            try {
                inFlightBudget.awaitCapacityOrDrain(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                inFlightBudget.cancelWaiting();
                return null;
            }
        }
    }

    /**
     * Target write gating & metering for business DML chunks.
     * Internal helper writes (e.g., exactly-once auxiliary caches) must NOT call this method.
     */
    public <E extends TapEvent> void executeTargetWriteChunked(
            String targetNodeId,
            String endpointId,
            ThrottlePhase phase,
            List<E> records,
            Function<TapEvent, String> tableNormalizer,
            boolean splittable,
            BooleanSupplier cancelled,
            ChunkWriter<E> writer) throws Exception {
        if (records == null || records.isEmpty()) {
            return;
        }
        if (fenced) {
            backpressureSignal.clearTargetWaiting(targetNodeId);
            throw buildFencedException("Target write");
        }
        if (closed || (cancelled != null && cancelled.getAsBoolean())) {
            backpressureSignal.clearTargetWaiting(targetNodeId);
            return;
        }
        ThrottlePhase resolvedPhase = phase == null ? ThrottlePhase.FULL : phase;
        aggregator.activateSlot(resolvedPhase, ThrottleDirection.OUTPUT, controller.getAppliedVersion());

        boolean phaseEnabled = controller.isPhaseEnabled(resolvedPhase);
        if (!phaseEnabled || !splittable || records.size() <= 1) {
            executeSingleTargetWriteChunk(
                    targetNodeId, endpointId, resolvedPhase, records, tableNormalizer, splittable && records.size() > 1, cancelled, writer);
            return;
        }

        long chunkMaxEvents = controller.maxSplittableEvents(resolvedPhase);
        long chunkMaxBytes = controller.maxSplittableBytes(resolvedPhase);
        BatchMeteringResult probe = meterBatch(outputMeter, resolvedPhase, endpointId, records, tableNormalizer, true);
        int sliceCount = computeSliceRecordCount(records.size(), probe, chunkMaxEvents, chunkMaxBytes);

        int offset = 0;
        while (offset < records.size()) {
            if (fenced) {
                backpressureSignal.clearTargetWaiting(targetNodeId);
                throw buildFencedException("Target write");
            }
            if (closed || (cancelled != null && cancelled.getAsBoolean())) {
                backpressureSignal.clearTargetWaiting(targetNodeId);
                return;
            }
            SubSlice<E> next = nextBoundedSlice(
                    records, offset, sliceCount, outputMeter, resolvedPhase, endpointId,
                    tableNormalizer, chunkMaxEvents, chunkMaxBytes, probe, true, false);
            if (!executeSingleTargetWriteChunk(
                    targetNodeId, endpointId, resolvedPhase, next.events(), tableNormalizer,
                    next.events().size() > 1, cancelled, writer, next.meter())) {
                return;
            }
            offset = next.nextOffset();
        }
    }

    private <E extends TapEvent> boolean executeSingleTargetWriteChunk(
            String targetNodeId,
            String endpointId,
            ThrottlePhase phase,
            List<E> chunk,
            Function<TapEvent, String> tableNormalizer,
            boolean splittable,
            BooleanSupplier cancelled,
            ChunkWriter<E> writer) throws Exception {
        return executeSingleTargetWriteChunk(targetNodeId, endpointId, phase, chunk, tableNormalizer, splittable, cancelled, writer, null);
    }

    private <E extends TapEvent> boolean executeSingleTargetWriteChunk(
            String targetNodeId,
            String endpointId,
            ThrottlePhase phase,
            List<E> chunk,
            Function<TapEvent, String> tableNormalizer,
            boolean splittable,
            BooleanSupplier cancelled,
            ChunkWriter<E> writer,
            BatchMeteringResult precomputedMeter) throws Exception {
        if (fenced) {
            backpressureSignal.clearTargetWaiting(targetNodeId);
            throw buildFencedException("Target write");
        }
        if (closed || (cancelled != null && cancelled.getAsBoolean())) {
            backpressureSignal.clearTargetWaiting(targetNodeId);
            return false;
        }
        BatchMeteringResult meter = precomputedMeter != null
                ? precomputedMeter
                : meterBatch(outputMeter, phase, endpointId, chunk, tableNormalizer, controller.isPhaseEnabled(phase));
        long appliedVersion = controller.getAppliedVersion();
        boolean tokenDebited = false;

        if (controller.isPhaseEnabled(phase)) {
            TaskThrottleController.WaitStateObserver observer = new TaskThrottleController.WaitStateObserver() {
                @Override
                public void onWaiting(TaskThrottleController.AcquireResult waitResult) {
                    WaitReason reason = waitResult.outcome() == TaskThrottleController.Outcome.SAMPLE_UNKNOWN_WAIT
                            ? WaitReason.SAMPLE_WAIT
                            : WaitReason.RATE_WAIT;
                    backpressureSignal.markTargetWaiting(targetNodeId, reason);
                    aggregator.updateWaitState(
                            phase,
                            ThrottleDirection.OUTPUT,
                            reason,
                            waitResult.waitingDimensions(),
                            inFlightBudget.currentWatermarkRatio(),
                            logRetentionMonitor.worstStatus()
                    );
                }

                @Override
                public void onAdmitted(TaskThrottleController.AcquireResult admittedResult) {
                    backpressureSignal.clearTargetWaiting(targetNodeId);
                    if (backpressureSignal.getWaitingTargetIds().isEmpty()) {
                        aggregator.updateWaitState(
                                phase,
                                ThrottleDirection.OUTPUT,
                                WaitReason.NONE,
                                EnumSet.noneOf(ThrottleDimension.class),
                                inFlightBudget.currentWatermarkRatio(),
                                logRetentionMonitor.worstStatus()
                        );
                    }
                }
            };
            MeteredAcquireResult acqResult = acquireQuotaWithSampleRetry(
                    phase, ThrottleDirection.OUTPUT, splittable, meter,
                    () -> meterBatch(outputMeter, phase, endpointId, chunk, tableNormalizer, true),
                    cancelled, observer);
            meter = acqResult.meter();
            TaskThrottleController.AcquireResult acq = acqResult.acquire();
            appliedVersion = acq.appliedVersion();
            if (acq.outcome() == TaskThrottleController.Outcome.CANCELLED || acq.outcome() == TaskThrottleController.Outcome.STOPPED) {
                backpressureSignal.clearTargetWaiting(targetNodeId);
                if (fenced) {
                    throw buildFencedException("Target write");
                }
                return false;
            }
            if (acq.outcome() == TaskThrottleController.Outcome.EXCEEDS_ATOMIC_LIMIT) {
                backpressureSignal.clearTargetWaiting(targetNodeId);
                throw new IllegalStateException("Output batch exceeds 5s atomic limit on " + acq.exceededDimension());
            }
            tokenDebited = true;
        }

        if (fenced) {
            backpressureSignal.clearTargetWaiting(targetNodeId);
            if (tokenDebited) {
                aggregator.recordCancelledAfterDebit(phase, ThrottleDirection.OUTPUT, meter.getDmlCount(), meter.getEstimatedBytes());
            }
            throw buildFencedException("Target write");
        }
        if (closed || (cancelled != null && cancelled.getAsBoolean())) {
            backpressureSignal.clearTargetWaiting(targetNodeId);
            if (tokenDebited) {
                aggregator.recordCancelledAfterDebit(phase, ThrottleDirection.OUTPUT, meter.getDmlCount(), meter.getEstimatedBytes());
            }
            return false;
        }

        aggregator.recordAdmitted(meter, appliedVersion);
        try {
            writer.write(chunk);
            backpressureSignal.clearTargetWaiting(targetNodeId);
            return true;
        } catch (Exception ex) {
            if (tokenDebited && cancelled != null && cancelled.getAsBoolean()) {
                aggregator.recordCancelledAfterDebit(phase, ThrottleDirection.OUTPUT, meter.getDmlCount(), meter.getEstimatedBytes());
            }
            backpressureSignal.markTargetWaiting(targetNodeId, WaitReason.WRITE_IO_WAIT);
            aggregator.updateWaitState(
                    phase,
                    ThrottleDirection.OUTPUT,
                    WaitReason.WRITE_IO_WAIT,
                    EnumSet.noneOf(ThrottleDimension.class),
                    inFlightBudget.currentWatermarkRatio(),
                    logRetentionMonitor.worstStatus()
            );
            throw ex;
        }
    }

    /**
     * Releases proportional capacity for processed target events and sweeps older source batches only
     * once every downstream target feeding from that source has passed the older batch sequence.
     */
    public void onTargetEventsCompleted(String targetNodeId, List<TapdataEvent> completedEvents) {
        if (completedEvents == null || completedEvents.isEmpty()) {
            return;
        }
        Map<String, Long> countByLease = new HashMap<>();
        Map<String, Long> maxSeqBySource = new HashMap<>();

        for (TapdataEvent ev : completedEvents) {
            if (ev == null) {
                continue;
            }
            Object leaseObj = ev.getInfo(INFO_KEY_LEASE_ID);
            if (leaseObj instanceof String leaseId && !leaseId.isEmpty()) {
                countByLease.merge(leaseId, 1L, Long::sum);
            }
            Object srcObj = ev.getInfo(INFO_KEY_SOURCE_NODE_ID);
            Object seqObj = ev.getInfo(INFO_KEY_BATCH_SEQ);
            if (srcObj instanceof String srcId && seqObj instanceof Number seqNum) {
                maxSeqBySource.merge(srcId, seqNum.longValue(), Math::max);
            }
        }

        for (Map.Entry<String, Long> entry : countByLease.entrySet()) {
            CapacityLease lease = inFlightBudget.getLease(entry.getKey());
            if (lease != null) {
                lease.ackProcessedEvents(targetNodeId, entry.getValue());
            }
        }

        String resolvedTarget = (targetNodeId == null || targetNodeId.isBlank()) ? "__default__" : targetNodeId;
        for (Map.Entry<String, Long> entry : maxSeqBySource.entrySet()) {
            String src = entry.getKey();
            long seq = entry.getValue();
            ConcurrentHashMap<String, Long> perTarget = maxCompletedSeqBySourceAndTarget.computeIfAbsent(
                    src, k -> new ConcurrentHashMap<>());
            perTarget.merge(resolvedTarget, seq, Math::max);

            Set<String> expectedTargets = sourceToTargets.get(src);
            long sweepWatermark;
            if (expectedTargets != null && !expectedTargets.isEmpty()) {
                long minAcrossAllTargets = Long.MAX_VALUE;
                boolean allTargetsObserved = true;
                for (String target : expectedTargets) {
                    Long targetMax = perTarget.get(target);
                    if (targetMax == null) {
                        allTargetsObserved = false;
                        break;
                    }
                    if (targetMax < minAcrossAllTargets) {
                        minAcrossAllTargets = targetMax;
                    }
                }
                if (!allTargetsObserved) {
                    continue;
                }
                sweepWatermark = minAcrossAllTargets;
            } else {
                sweepWatermark = seq;
            }
            if (sweepWatermark > 1L) {
                inFlightBudget.flushOlderLeasesForSource(src, sweepWatermark);
            }
        }
        backpressureSignal.signalDrainProgress();
    }

    /**
     * Gracefully closes new admissions, wakes waiters, reclaims all remaining in-flight leases,
     * and creates a verifiable stop evidence payload for TM persistence.
     */
    public TaskThrottleStopEvidenceDto shutdownAndBuildStopEvidence(
            String currentAgentId,
            boolean writesSettled,
            String note) {
        this.closed = true;
        if (currentAgentId != null && !currentAgentId.isBlank()) {
            this.agentId = currentAgentId;
        }
        controller.shutdown();
        inFlightBudget.invalidateAndReclaimAll();
        backpressureSignal.signalDrainProgress();

        long now = clock.currentTimeMillis();
        return TaskThrottleStopEvidenceDto.builder()
                .tenantId(runKey.getTenantId())
                .taskKind(runKey.getTaskKind())
                .taskId(runKey.getTaskId())
                .oldAgentId(this.agentId)
                .oldRunEpoch(runKey.getRunEpoch())
                .source(TaskThrottleStopEvidenceDto.SOURCE_AGENT_GRACEFUL_STOP)
                .evidenceRef(runKey.toString() + "#" + now)
                .observedAt(now)
                .submittedBy("agent:" + (this.agentId == null ? "unknown" : this.agentId))
                .processExited(false)
                .writeSettled(writesSettled)
                .note(note == null ? "Agent closed task runtime cleanly" : note)
                .build();
    }

    public List<TaskThrottleTelemetrySample> snapshotAndRollTelemetry() {
        return aggregator.snapshotAndRollWindow();
    }

    public static void attachLeaseInfo(TapdataEvent tapdataEvent, String leaseId, String sourceNodeId, long batchSeq) {
        if (tapdataEvent == null || leaseId == null) {
            return;
        }
        Map<String, Object> info = new HashMap<>(4);
        info.put(INFO_KEY_LEASE_ID, leaseId);
        if (sourceNodeId != null) {
            info.put(INFO_KEY_SOURCE_NODE_ID, sourceNodeId);
        }
        info.put(INFO_KEY_BATCH_SEQ, batchSeq);
        tapdataEvent.addInfos(info);
    }

    public static ThrottlePhase resolvePhaseFromSyncStage(SyncStage syncStage, ThrottlePhase fallback) {
        if (syncStage == SyncStage.CDC) {
            return ThrottlePhase.CDC;
        }
        if (syncStage == SyncStage.INITIAL_SYNC) {
            return ThrottlePhase.FULL;
        }
        return fallback != null ? fallback : ThrottlePhase.FULL;
    }

    private BatchMeteringResult meterBatch(
            ContinuousBatchMeter meter,
            ThrottlePhase phase,
            String endpointId,
            List<? extends TapEvent> events,
            Function<TapEvent, String> tableNormalizer,
            boolean rateWaiting) {
        if (phase == ThrottlePhase.CDC) {
            return meter.meterCdcBatch(endpointId, events, tableNormalizer, rateWaiting);
        }
        return meter.meterFullBatch(endpointId, events, tableNormalizer);
    }

    private MeteredAcquireResult acquireQuotaWithSampleRetry(
            ThrottlePhase phase,
            ThrottleDirection direction,
            boolean splittable,
            BatchMeteringResult initialMeter,
            Supplier<BatchMeteringResult> remeterSupplier,
            BooleanSupplier cancelled,
            TaskThrottleController.WaitStateObserver observer) {
        BatchMeteringResult currentMeter = initialMeter;
        TaskThrottleController.PhaseRule rule = controller.currentSnapshot().phases().get(phase);
        boolean bytesOn = rule != null && rule.bytesEnabled();

        if (bytesOn && currentMeter.getEstimatedBytes() == null) {
            if (phase == ThrottlePhase.CDC) {
                for (int attempt = 0; attempt < MAX_SAMPLE_UNKNOWN_RETRIES; attempt++) {
                    if (fenced || closed) {
                        return new MeteredAcquireResult(
                                TaskThrottleController.AcquireResult.stopped(controller.getAppliedVersion()), currentMeter);
                    }
                    if (cancelled != null && cancelled.getAsBoolean()) {
                        return new MeteredAcquireResult(
                                TaskThrottleController.AcquireResult.cancelled(controller.getAppliedVersion()), currentMeter);
                    }
                    if (observer != null) {
                        observer.onWaiting(TaskThrottleController.AcquireResult.waitForUnknownSample(
                                controller.getAppliedVersion(), EnumSet.of(ThrottleDimension.BYTES)));
                    }
                    sleepSampleRetryStep(SAMPLE_UNKNOWN_RETRY_SLEEP_MS);
                    if (fenced || closed) {
                        return new MeteredAcquireResult(
                                TaskThrottleController.AcquireResult.stopped(controller.getAppliedVersion()), currentMeter);
                    }
                    if (cancelled != null && cancelled.getAsBoolean()) {
                        return new MeteredAcquireResult(
                                TaskThrottleController.AcquireResult.cancelled(controller.getAppliedVersion()), currentMeter);
                    }
                    currentMeter = remeterSupplier.get();
                    if (currentMeter.getEstimatedBytes() != null) {
                        break;
                    }
                }
            }
            if (currentMeter.getEstimatedBytes() == null) {
                logger.warn("Degrading unknown byte metering to QPS-only/unthrottled pass for task={} phase={} dir={}",
                        runKey, phase, direction);
            }
        }

        // Known null bytes after bounded CDC retries or FULL unmeterables bypass BYTES blocking without spinning.
        long bytesCost = currentMeter.getEstimatedBytes() == null ? 0L : Math.max(0L, currentMeter.getEstimatedBytes());
        long eventsCost = currentMeter.getDmlCount();
        BooleanSupplier stopSignal = () -> fenced || closed || (cancelled != null && cancelled.getAsBoolean());
        TaskThrottleController.AcquireResult acq = controller.acquireBlocking(
                Collections.singletonList(new TaskThrottleController.SlotDemand(
                        phase, direction, bytesCost, eventsCost, splittable, true, false)),
                stopSignal,
                observer
        );
        if (acq.outcome() == TaskThrottleController.Outcome.EXCEEDS_ATOMIC_LIMIT) {
            logger.warn("Indivisible unit exceeds 5s atomic limit on {} (task={}, phase={}, dir={}, cost={}, limit={}); admitting via bounded-wait overdraft debt",
                    acq.exceededDimension(), runKey, phase, direction, acq.requestedCost(), acq.limitCost());
            acq = controller.acquireBlocking(
                    Collections.singletonList(new TaskThrottleController.SlotDemand(
                            phase, direction, bytesCost, eventsCost, splittable, true, true)),
                    stopSignal,
                    observer
            );
        }
        return new MeteredAcquireResult(acq, currentMeter);
    }

    private static void sleepSampleRetryStep(long sleepMillis) {
        if (sleepMillis <= 0L) {
            return;
        }
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private <T extends TapEvent> SubSlice<T> nextBoundedSlice(
            List<T> events,
            int offset,
            int targetCount,
            ContinuousBatchMeter meter,
            ThrottlePhase phase,
            String endpointId,
            Function<TapEvent, String> tableNormalizer,
            long chunkMaxEvents,
            long chunkMaxBytes,
            BatchMeteringResult fullBatchProbe,
            boolean rateWaiting,
            boolean enforceBudgetFit) {
        int curCount = Math.max(1, Math.min(targetCount, events.size() - offset));
        while (true) {
            int end = Math.min(events.size(), offset + curCount);
            List<T> subEvents = (offset == 0 && end == events.size()) ? events : new ArrayList<>(events.subList(offset, end));
            BatchMeteringResult sliceMeter = (offset == 0 && end == events.size())
                    ? fullBatchProbe
                    : meterBatch(meter, phase, endpointId, subEvents, tableNormalizer, rateWaiting);
            boolean overBytes = chunkMaxBytes > 0L && chunkMaxBytes < Long.MAX_VALUE
                    && sliceMeter.getEstimatedBytes() != null
                    && sliceMeter.getEstimatedBytes() > chunkMaxBytes;
            boolean overEvents = chunkMaxEvents > 0L && chunkMaxEvents < Long.MAX_VALUE
                    && sliceMeter.getDmlCount() > chunkMaxEvents;
            boolean overBudget = enforceBudgetFit
                    && !inFlightBudget.canEverFit(
                    estimateConservativeMemoryBytes(sliceMeter, subEvents.size()),
                    subEvents.size(),
                    1L);
            if (subEvents.size() <= 1 || (!overBytes && !overEvents && !overBudget)) {
                return new SubSlice<>(subEvents, end, sliceMeter);
            }
            curCount = Math.max(1, subEvents.size() / 2);
        }
    }

    private record MeteredAcquireResult(TaskThrottleController.AcquireResult acquire, BatchMeteringResult meter) {
    }

    private record SubSlice<T extends TapEvent>(List<T> events, int nextOffset, BatchMeteringResult meter) {
    }

    private int computeSliceRecordCount(int totalSize, BatchMeteringResult probe, long maxEvents, long maxBytes) {
        int limit = totalSize;
        if (maxEvents > 0L && maxEvents < limit) {
            limit = (int) Math.max(1L, maxEvents);
        }
        if (maxBytes > 0L && maxBytes < Long.MAX_VALUE && probe.getEstimatedBytes() != null && probe.getDmlCount() > 0L) {
            double avgBytesPerEvent = Math.max(1.0d, (double) probe.getEstimatedBytes() / probe.getDmlCount());
            int byteCapCount = (int) Math.max(1L, (long) Math.floor(maxBytes / avgBytesPerEvent));
            if (byteCapCount < limit) {
                limit = byteCapCount;
            }
        }
        return Math.max(1, limit);
    }

    private long estimateConservativeMemoryBytes(BatchMeteringResult meter, int recordCount) {
        long minFloor = Math.max(1L, recordCount) * MIN_FALLBACK_RECORD_MEM_BYTES;
        if (meter == null || meter.getEstimatedBytes() == null) {
            return minFloor;
        }
        try {
            return Math.max(minFloor, Math.multiplyExact(meter.getEstimatedBytes(), 2L));
        } catch (ArithmeticException ex) {
            return Math.max(minFloor, meter.getEstimatedBytes());
        }
    }

    public enum AdmitOutcome {
        ADMITTED,
        CANCELLED,
        FENCED_OR_STOPPED
    }

    public record AdmittedSourceSlice<T extends TapEvent>(
            List<T> events,
            CapacityLease lease,
            String sourceNodeId,
            long batchSeq) {
    }

    @FunctionalInterface
    public interface ChunkWriter<E extends TapEvent> {
        void write(List<E> chunk) throws Exception;
    }
}
