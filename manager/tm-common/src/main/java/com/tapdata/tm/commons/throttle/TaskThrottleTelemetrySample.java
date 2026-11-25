package com.tapdata.tm.commons.throttle;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

/**
 * Structured 5-second metering and throttling telemetry snapshot per (phase, direction).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TaskThrottleTelemetrySample implements Serializable {
    private static final long serialVersionUID = 1L;

    private long runEpoch;
    private long sampleSeq;
    private String meterVersion;
    private long sampleConfigVersion;
    private int cdcSampleRefreshIntervalSeconds;
    private ThrottlePhase phase;
    private ThrottleDirection direction;
    private long windowStartUtc;
    private long windowEndUtc;
    private long durationNanos;
    private TimeQuality timeQuality;
    private long appliedVersion;
    private long admittedEvents;
    private Long estimatedAdmittedBytes;
    private long sampleCount;
    private SampleQuality sampleQuality;
    private long cancelledAfterDebitEvents;
    private Long cancelledEstimatedBytes;
    private WaitReason waitReason;
    private Set<ThrottleDimension> waitingDimensions;
    private double bufferWatermark;
    private LogPositionStatus logPositionStatus;
}
