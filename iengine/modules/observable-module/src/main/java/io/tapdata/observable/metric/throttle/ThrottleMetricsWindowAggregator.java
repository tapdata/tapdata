package io.tapdata.observable.metric.throttle;

import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.TimeQuality;
import com.tapdata.tm.commons.throttle.WaitReason;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 5-second window telemetry aggregator driven by monotonic time projected against an anchored UTC wall clock.
 * Distinguishes valid zero-byte intervals from missing byte measurements and detects UTC clock jumps beyond 2s.
 */
public final class ThrottleMetricsWindowAggregator {

    private static final long DRIFT_THRESHOLD_MILLIS = TimeUnit.SECONDS.toMillis(ThrottleConstants.CLOCK_DRIFT_WARN_SECONDS);

    private final long runEpoch;
    private final String meterVersion;
    private final MonotonicClock clock;

    private final EnumMap<ThrottlePhase, EnumMap<ThrottleDirection, SlotAccumulator>> slots = new EnumMap<>(ThrottlePhase.class);
    private final EnumMap<ThrottleDirection, DirectionSnapshot> latestRates = new EnumMap<>(ThrottleDirection.class);

    private long anchorMonoNanos;
    private long anchorUtcMillis;
    private long windowStartMonoNanos;
    private long lastWindowEndUtc;
    private long sampleSeq;

    private volatile long latestSampleConfigVersion = 1L;
    private volatile int latestCdcSampleIntervalSec = ThrottleConstants.DEFAULT_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS;
    private volatile long latestAppliedVersion = 0L;
    private volatile WaitReason latestWaitReason = WaitReason.NONE;
    private volatile int latestWaitingDimMask = 0;
    private volatile double latestBufferWatermark = 0.0d;
    private volatile SampleQuality latestSampleQuality = SampleQuality.VALID;
    private volatile LogPositionStatus latestLogPositionStatus = LogPositionStatus.NORMAL;
    private volatile long cumulativeCancelledEvents = 0L;
    private volatile Long cumulativeCancelledBytes = 0L;

    public ThrottleMetricsWindowAggregator(long runEpoch, MonotonicClock clock) {
        this(runEpoch, ThrottleConstants.METER_VERSION, clock);
    }

    public ThrottleMetricsWindowAggregator(long runEpoch, String meterVersion, MonotonicClock clock) {
        if (clock == null) {
            throw new IllegalArgumentException("MonotonicClock must not be null");
        }
        this.runEpoch = runEpoch;
        this.meterVersion = meterVersion != null ? meterVersion : ThrottleConstants.METER_VERSION;
        this.clock = clock;
        long mono = clock.nanoTime();
        long utc = clock.currentTimeMillis();
        this.anchorMonoNanos = mono;
        this.anchorUtcMillis = utc;
        this.windowStartMonoNanos = mono;
        this.lastWindowEndUtc = utc;
        for (ThrottlePhase phase : ThrottlePhase.values()) {
            EnumMap<ThrottleDirection, SlotAccumulator> byDir = new EnumMap<>(ThrottleDirection.class);
            for (ThrottleDirection dir : ThrottleDirection.values()) {
                byDir.put(dir, new SlotAccumulator(phase, dir));
            }
            slots.put(phase, byDir);
        }
    }

    public synchronized void activateSlot(ThrottlePhase phase, ThrottleDirection direction, long appliedVersion) {
        if (phase == null || direction == null) {
            return;
        }
        SlotAccumulator acc = slots.get(phase).get(direction);
        acc.active = true;
        acc.persistZeroIdle = true;
        acc.appliedVersion = appliedVersion;
    }

    public synchronized void deactivatePhase(ThrottlePhase phase) {
        if (phase == null) {
            return;
        }
        for (SlotAccumulator acc : slots.get(phase).values()) {
            acc.persistZeroIdle = false;
            if (acc.admittedEvents == 0L && !acc.missingBytes && acc.waitReason == WaitReason.NONE) {
                acc.active = false;
            }
        }
    }

    public synchronized void recordAdmitted(BatchMeteringResult result, long appliedVersion) {
        if (result == null) {
            return;
        }
        SlotAccumulator acc = slots.get(result.getPhase()).get(result.getDirection());
        acc.active = true;
        acc.persistZeroIdle = true;
        acc.appliedVersion = appliedVersion;
        acc.sampleConfigVersion = result.getSampleConfigVersion();
        acc.cdcSampleIntervalSec = result.getCdcSampleRefreshIntervalSeconds();
        acc.admittedEvents += result.getDmlCount();
        acc.sampleCount += result.getSampleCount();
        if (result.getEstimatedBytes() == null) {
            acc.missingBytes = true;
        } else if (!acc.missingBytes) {
            acc.estimatedAdmittedBytes += result.getEstimatedBytes();
        }
        acc.sampleQuality = CdcTableDmlSampleCache.combineQuality(acc.sampleQuality, result.getSampleQuality());

        this.latestAppliedVersion = appliedVersion;
        this.latestSampleConfigVersion = result.getSampleConfigVersion();
        this.latestCdcSampleIntervalSec = result.getCdcSampleRefreshIntervalSeconds();
        this.latestSampleQuality = acc.sampleQuality;
    }

    public synchronized void recordCancelledAfterDebit(
            ThrottlePhase phase,
            ThrottleDirection direction,
            long events,
            Long estimatedBytes) {
        if (phase == null || direction == null) {
            return;
        }
        SlotAccumulator acc = slots.get(phase).get(direction);
        acc.active = true;
        acc.cancelledEvents += Math.max(0L, events);
        if (estimatedBytes == null) {
            acc.cancelledBytesMissing = true;
            this.cumulativeCancelledBytes = null;
        } else if (!acc.cancelledBytesMissing) {
            acc.cancelledBytes += Math.max(0L, estimatedBytes);
            if (this.cumulativeCancelledBytes != null) {
                this.cumulativeCancelledBytes += Math.max(0L, estimatedBytes);
            }
        }
        this.cumulativeCancelledEvents += Math.max(0L, events);
    }

    public synchronized void updateWaitState(
            ThrottlePhase phase,
            ThrottleDirection direction,
            WaitReason reason,
            Set<ThrottleDimension> waitingDimensions,
            double bufferWatermark,
            LogPositionStatus logPositionStatus) {
        if (phase == null || direction == null) {
            return;
        }
        SlotAccumulator acc = slots.get(phase).get(direction);
        acc.active = true;
        acc.waitReason = reason != null ? reason : WaitReason.NONE;
        acc.waitingDimensions = waitingDimensions == null || waitingDimensions.isEmpty()
                ? EnumSet.noneOf(ThrottleDimension.class)
                : EnumSet.copyOf(waitingDimensions);
        acc.bufferWatermark = bufferWatermark;
        acc.logPositionStatus = logPositionStatus != null ? logPositionStatus : LogPositionStatus.NORMAL;

        this.latestWaitReason = acc.waitReason;
        this.latestWaitingDimMask = ThrottleDimension.toMask(acc.waitingDimensions);
        this.latestBufferWatermark = bufferWatermark;
        this.latestLogPositionStatus = acc.logPositionStatus;
    }

    public synchronized List<TaskThrottleTelemetrySample> snapshotAndRollWindow() {
        long nowMono = clock.nanoTime();
        long actualUtc = clock.currentTimeMillis();
        long rawDuration = nowMono - windowStartMonoNanos;

        TimeQuality timeQuality;
        long durationNanos;
        long windowStartUtc = lastWindowEndUtc;
        long windowEndUtc;

        if (rawDuration < 0L) {
            timeQuality = TimeQuality.UNALIGNED;
            durationNanos = 0L;
            anchorMonoNanos = nowMono;
            anchorUtcMillis = Math.max(lastWindowEndUtc, actualUtc);
            windowEndUtc = anchorUtcMillis;
        } else {
            durationNanos = rawDuration;
            long elapsedFromAnchorMs = Math.max(0L, (nowMono - anchorMonoNanos) / 1_000_000L);
            long projectedEndUtc = anchorUtcMillis + elapsedFromAnchorMs;
            if (Math.abs(actualUtc - projectedEndUtc) > DRIFT_THRESHOLD_MILLIS) {
                timeQuality = TimeQuality.DRIFT_DETECTED;
                anchorMonoNanos = nowMono;
                anchorUtcMillis = Math.max(windowStartUtc, actualUtc);
                windowEndUtc = anchorUtcMillis;
            } else {
                timeQuality = TimeQuality.ALIGNED;
                windowEndUtc = Math.max(windowStartUtc, projectedEndUtc);
            }
        }

        windowStartMonoNanos = nowMono;
        lastWindowEndUtc = windowEndUtc;
        long seq = ++sampleSeq;

        long inputBytesSum = 0L;
        boolean inputBytesMissing = false;
        boolean hasInputSlot = false;
        long outputBytesSum = 0L;
        boolean outputBytesMissing = false;
        boolean hasOutputSlot = false;

        List<TaskThrottleTelemetrySample> samples = new ArrayList<>(4);
        for (ThrottlePhase phase : ThrottlePhase.values()) {
            for (ThrottleDirection dir : ThrottleDirection.values()) {
                SlotAccumulator acc = slots.get(phase).get(dir);
                if (!acc.active && acc.waitReason == WaitReason.NONE) {
                    continue;
                }
                Long bytesValue = acc.missingBytes ? null : acc.estimatedAdmittedBytes;
                Long cancelledBytesValue = acc.cancelledBytesMissing ? null : acc.cancelledBytes;

                samples.add(TaskThrottleTelemetrySample.builder()
                        .runEpoch(runEpoch)
                        .sampleSeq(seq)
                        .meterVersion(meterVersion)
                        .sampleConfigVersion(acc.sampleConfigVersion)
                        .cdcSampleRefreshIntervalSeconds(acc.cdcSampleIntervalSec)
                        .phase(phase)
                        .direction(dir)
                        .windowStartUtc(windowStartUtc)
                        .windowEndUtc(windowEndUtc)
                        .durationNanos(durationNanos)
                        .timeQuality(timeQuality)
                        .appliedVersion(acc.appliedVersion)
                        .admittedEvents(acc.admittedEvents)
                        .estimatedAdmittedBytes(bytesValue)
                        .sampleCount(acc.sampleCount)
                        .sampleQuality(acc.sampleQuality)
                        .cancelledAfterDebitEvents(acc.cancelledEvents)
                        .cancelledEstimatedBytes(cancelledBytesValue)
                        .waitReason(acc.waitReason)
                        .waitingDimensions(Collections.unmodifiableSet(EnumSet.copyOf(acc.waitingDimensions)))
                        .bufferWatermark(acc.bufferWatermark)
                        .logPositionStatus(acc.logPositionStatus)
                        .build());

                if (dir == ThrottleDirection.INPUT) {
                    hasInputSlot = true;
                    if (bytesValue == null) {
                        inputBytesMissing = true;
                    } else if (!inputBytesMissing) {
                        inputBytesSum += bytesValue;
                    }
                } else {
                    hasOutputSlot = true;
                    if (bytesValue == null) {
                        outputBytesMissing = true;
                    } else if (!outputBytesMissing) {
                        outputBytesSum += bytesValue;
                    }
                }
                acc.rollWindow();
            }
        }

        double seconds = durationNanos > 0L ? durationNanos / 1_000_000_000.0d : 0.0d;
        latestRates.put(ThrottleDirection.INPUT, toRateSnapshot(inputBytesMissing, inputBytesSum, seconds));
        latestRates.put(ThrottleDirection.OUTPUT, toRateSnapshot(outputBytesMissing, outputBytesSum, seconds));

        return Collections.unmodifiableList(samples);
    }

    private static DirectionSnapshot toRateSnapshot(boolean missing, long bytesSum, double seconds) {
        if (missing) {
            return new DirectionSnapshot(null);
        }
        double rate = seconds > 0.0d ? (bytesSum / seconds) : 0.0d;
        return new DirectionSnapshot(rate);
    }

    /**
     * Read-only numeric map safe for SampleCollector reporters (returns null for missing byte estimates vs 0.0 for valid zero).
     */
    public synchronized Map<String, Number> latestNumericMetrics() {
        Map<String, Number> metrics = new HashMap<>();
        DirectionSnapshot inputRate = latestRates.get(ThrottleDirection.INPUT);
        DirectionSnapshot outputRate = latestRates.get(ThrottleDirection.OUTPUT);
        metrics.put(MetricCons.SS.VS.F_ESTIMATED_INPUT_BYTES_RATE, inputRate == null ? Double.valueOf(0.0d) : inputRate.bytesPerSec);
        metrics.put(MetricCons.SS.VS.F_ESTIMATED_OUTPUT_BYTES_RATE, outputRate == null ? Double.valueOf(0.0d) : outputRate.bytesPerSec);
        metrics.put(MetricCons.SS.VS.F_THROTTLE_APPLIED_VERSION, latestAppliedVersion);
        metrics.put(MetricCons.SS.VS.F_SAMPLE_CONFIG_VERSION, latestSampleConfigVersion);
        metrics.put(MetricCons.SS.VS.F_CDC_SAMPLE_INTERVAL_SEC, latestCdcSampleIntervalSec);
        metrics.put(MetricCons.SS.VS.F_THROTTLE_RUN_EPOCH, runEpoch);
        metrics.put(MetricCons.SS.VS.F_THROTTLE_SAMPLE_SEQ, sampleSeq);
        metrics.put(MetricCons.SS.VS.F_THROTTLE_WAIT_REASON_CODE, latestWaitReason.code());
        metrics.put(MetricCons.SS.VS.F_THROTTLE_WAITING_DIM_MASK, latestWaitingDimMask);
        metrics.put(MetricCons.SS.VS.F_THROTTLE_BUFFER_WATERMARK, latestBufferWatermark);
        metrics.put(MetricCons.SS.VS.F_THROTTLE_SAMPLE_QUALITY_CODE, latestSampleQuality.code());
        metrics.put(MetricCons.SS.VS.F_THROTTLE_LOG_POSITION_STATUS_CODE, latestLogPositionStatus.code());
        metrics.put(MetricCons.SS.VS.F_CANCELLED_AFTER_DEBIT_EVENTS, cumulativeCancelledEvents);
        metrics.put(MetricCons.SS.VS.F_CANCELLED_ESTIMATED_BYTES, cumulativeCancelledBytes);
        return Collections.unmodifiableMap(metrics);
    }

    private record DirectionSnapshot(Double bytesPerSec) {
    }

    private final class SlotAccumulator {
        final ThrottlePhase phase;
        final ThrottleDirection direction;
        boolean active;
        boolean persistZeroIdle;
        long appliedVersion = 0L;
        long sampleConfigVersion = latestSampleConfigVersion;
        int cdcSampleIntervalSec = latestCdcSampleIntervalSec;
        long admittedEvents = 0L;
        long estimatedAdmittedBytes = 0L;
        boolean missingBytes = false;
        long sampleCount = 0L;
        SampleQuality sampleQuality = SampleQuality.VALID;
        long cancelledEvents = 0L;
        long cancelledBytes = 0L;
        boolean cancelledBytesMissing = false;
        WaitReason waitReason = WaitReason.NONE;
        EnumSet<ThrottleDimension> waitingDimensions = EnumSet.noneOf(ThrottleDimension.class);
        double bufferWatermark = 0.0d;
        LogPositionStatus logPositionStatus = LogPositionStatus.NORMAL;

        SlotAccumulator(ThrottlePhase phase, ThrottleDirection direction) {
            this.phase = phase;
            this.direction = direction;
        }

        void rollWindow() {
            this.admittedEvents = 0L;
            this.estimatedAdmittedBytes = 0L;
            this.missingBytes = false;
            this.sampleCount = 0L;
            this.sampleQuality = SampleQuality.VALID;
            this.cancelledEvents = 0L;
            this.cancelledBytes = 0L;
            this.cancelledBytesMissing = false;
            // Keep slot active when idle-waiting or running phase persists for zero-rate continuity
            this.active = this.persistZeroIdle || this.waitReason != WaitReason.NONE;
        }
    }
}
