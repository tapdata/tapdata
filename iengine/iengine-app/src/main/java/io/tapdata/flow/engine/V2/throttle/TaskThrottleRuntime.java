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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

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
    private final ConcurrentHashMap<String, Long> maxCompletedSeqBySource = new ConcurrentHashMap<>();

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
        this.backpressureSignal = new WriteBackpressureSignal(this.inFlightBudget);
        this.logRetentionMonitor = new LogRetentionMonitor();
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
        if (!protectedRun || baseCollection == null) {
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
        if (dto != null && dto.getPhases() != null && dto.getPhases().values().stream().anyMatch(p -> p != null && p.isEnabled())) {
            this.protectedRun = true;
        }
        return receipt;
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

        if (!phaseEnabled) {
            // Metering-only + capacity protection path: keep original batch unsplit.
            BatchMeteringResult meter = meterBatch(inputMeter, resolvedPhase, endpointId, events, tableNormalizer, false);
            return admitSingleSlice(sourceNodeId, resolvedPhase, events, meter, true, cancelled, sliceDispatcher);
        }

        // Throttling enabled: slice large splittable batches so each piece stays within QPS/BYTES burst limits.
        long chunkMaxEvents = controller.maxSplittableEvents(resolvedPhase);
        long chunkMaxBytes = controller.maxSplittableBytes(resolvedPhase);

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
            int end = Math.min(events.size(), offset + estimatedSliceSize);
            List<T> subEvents = (offset == 0 && end == events.size()) ? events : new ArrayList<>(events.subList(offset, end));
            BatchMeteringResult sliceMeter = (offset == 0 && end == events.size())
                    ? probe
                    : meterBatch(inputMeter, resolvedPhase, endpointId, subEvents, tableNormalizer, true);
            boolean splittable = subEvents.size() > 1;
            AdmitOutcome outcome = admitSingleSlice(
                    sourceNodeId, resolvedPhase, subEvents, sliceMeter, splittable, cancelled, sliceDispatcher);
            if (outcome != AdmitOutcome.ADMITTED) {
                return outcome;
            }
            offset = end;
        }
        return AdmitOutcome.ADMITTED;
    }

    private <T extends TapEvent> AdmitOutcome admitSingleSlice(
            String sourceNodeId,
            ThrottlePhase phase,
            List<T> sliceEvents,
            BatchMeteringResult meterResult,
            boolean splittable,
            BooleanSupplier cancelled,
            Consumer<AdmittedSourceSlice<T>> sliceDispatcher) {
        long seq = batchSeqGenerator.getAndIncrement();
        long memBytes = estimateConservativeMemoryBytes(meterResult, sliceEvents.size());
        long recordCount = sliceEvents.size();

        if (!inFlightBudget.canEverFit(memBytes, recordCount, 1L)) {
            throw new IllegalStateException("Batch exceeds hard in-flight capacity budget: bytes=" + memBytes + ", records=" + recordCount);
        }

        CapacityLease lease = acquireCapacityWithBackpressure(sourceNodeId, seq, phase, memBytes, recordCount, cancelled);
        if (lease == null) {
            return (fenced || closed) ? AdmitOutcome.FENCED_OR_STOPPED : AdmitOutcome.CANCELLED;
        }

        boolean leaseTransferredToDispatcher = false;
        long appliedVersion = controller.getAppliedVersion();
        boolean tokenDebited = false;
        try {
            if (controller.isPhaseEnabled(phase)) {
                TaskThrottleController.PolicySnapshot snap = controller.currentSnapshot();
                TaskThrottleController.PhaseRule rule = snap.phases().get(phase);
                boolean bytesOn = rule != null && rule.bytesEnabled();

                long bytesCost = meterResult.getEstimatedBytes() == null ? 0L : Math.max(0L, meterResult.getEstimatedBytes());
                long eventsCost = meterResult.getDmlCount();
                boolean bytesCostKnown = !bytesOn || meterResult.getEstimatedBytes() != null;

                TaskThrottleController.AcquireResult acq = controller.acquireBlocking(
                        Collections.singletonList(new TaskThrottleController.SlotDemand(
                                phase, ThrottleDirection.INPUT, bytesCost, eventsCost, splittable, bytesCostKnown)),
                        () -> fenced || closed || (cancelled != null && cancelled.getAsBoolean()),
                        new TaskThrottleController.WaitStateObserver() {
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
                        }
                );

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
                            phase, ThrottleDirection.INPUT, meterResult.getDmlCount(), meterResult.getEstimatedBytes());
                }
                return (fenced || closed) ? AdmitOutcome.FENCED_OR_STOPPED : AdmitOutcome.CANCELLED;
            }

            aggregator.recordAdmitted(meterResult, appliedVersion);
            if (sliceDispatcher != null) {
                lease.transferOwnership(lease.getHolderVersion(), "pipeline-queue");
                sliceDispatcher.accept(new AdmittedSourceSlice<>(sliceEvents, lease, sourceNodeId, seq));
                leaseTransferredToDispatcher = true;
            }
            return AdmitOutcome.ADMITTED;
        } catch (RuntimeException ex) {
            if (tokenDebited && !leaseTransferredToDispatcher) {
                aggregator.recordCancelledAfterDebit(
                        phase, ThrottleDirection.INPUT, meterResult.getDmlCount(), meterResult.getEstimatedBytes());
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
                    memBytes, recordCount, 1L, sourceNodeId, sourceNodeId, batchSeq, true);
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
        if (fenced || closed) {
            throw new IllegalStateException("Target write rejected for fenced/stopped runEpoch: " + runKey);
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
            if (fenced || closed) {
                throw new IllegalStateException("Target write aborted for fenced/stopped runEpoch: " + runKey);
            }
            int end = Math.min(records.size(), offset + sliceCount);
            List<E> sub = (offset == 0 && end == records.size()) ? records : new ArrayList<>(records.subList(offset, end));
            executeSingleTargetWriteChunk(
                    targetNodeId, endpointId, resolvedPhase, sub, tableNormalizer, sub.size() > 1, cancelled, writer);
            offset = end;
        }
    }

    private <E extends TapEvent> void executeSingleTargetWriteChunk(
            String targetNodeId,
            String endpointId,
            ThrottlePhase phase,
            List<E> chunk,
            Function<TapEvent, String> tableNormalizer,
            boolean splittable,
            BooleanSupplier cancelled,
            ChunkWriter<E> writer) throws Exception {
        BatchMeteringResult meter = meterBatch(outputMeter, phase, endpointId, chunk, tableNormalizer, controller.isPhaseEnabled(phase));
        long appliedVersion = controller.getAppliedVersion();
        boolean tokenDebited = false;

        if (controller.isPhaseEnabled(phase)) {
            TaskThrottleController.PhaseRule rule = controller.currentSnapshot().phases().get(phase);
            boolean bytesOn = rule != null && rule.bytesEnabled();
            long bytesCost = meter.getEstimatedBytes() == null ? 0L : Math.max(0L, meter.getEstimatedBytes());
            long eventsCost = meter.getDmlCount();
            boolean bytesCostKnown = !bytesOn || meter.getEstimatedBytes() != null;

            TaskThrottleController.AcquireResult acq = controller.acquireBlocking(
                    Collections.singletonList(new TaskThrottleController.SlotDemand(
                            phase, ThrottleDirection.OUTPUT, bytesCost, eventsCost, splittable, bytesCostKnown)),
                    () -> fenced || closed || (cancelled != null && cancelled.getAsBoolean()),
                    new TaskThrottleController.WaitStateObserver() {
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
            );
            appliedVersion = acq.appliedVersion();
            if (acq.outcome() == TaskThrottleController.Outcome.CANCELLED || acq.outcome() == TaskThrottleController.Outcome.STOPPED) {
                backpressureSignal.clearTargetWaiting(targetNodeId);
                throw new IllegalStateException("Target write cancelled/stopped while waiting output quota");
            }
            if (acq.outcome() == TaskThrottleController.Outcome.EXCEEDS_ATOMIC_LIMIT) {
                backpressureSignal.clearTargetWaiting(targetNodeId);
                throw new IllegalStateException("Output batch exceeds 5s atomic limit on " + acq.exceededDimension());
            }
            tokenDebited = true;
        }

        aggregator.recordAdmitted(meter, appliedVersion);
        try {
            writer.write(chunk);
            backpressureSignal.clearTargetWaiting(targetNodeId);
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
     * Releases proportional capacity for processed target events and flushes any older source batches
     * filtered out earlier in the DAG so watermarks drain deterministically.
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
                lease.ackProcessedEvents(entry.getValue());
            }
        }

        for (Map.Entry<String, Long> entry : maxSeqBySource.entrySet()) {
            String src = entry.getKey();
            long seq = entry.getValue();
            maxCompletedSeqBySource.merge(src, seq, Math::max);
            if (seq > 1L) {
                inFlightBudget.flushOlderLeasesForSource(src, seq);
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
