package com.tapdata.tm.throttle.entity;

import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.TimeQuality;
import com.tapdata.tm.commons.throttle.WaitReason;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Set;

/**
 * Deduplicated 5-second metering & throttling window sample uploaded by Agents.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document("TaskThrottleTelemetry")
public class TaskThrottleTelemetryEntity {

    @Id
    private String id;

    private String tenantId;
    private String taskKind;
    @Indexed
    private String taskId;
    private String agentId;
    private long receivedAtUtc;

    private long runEpoch;
    private long sampleSeq;
    private String meterVersion;
    private long sampleConfigVersion;
    private int cdcSampleRefreshIntervalSeconds;
    private ThrottlePhase phase;
    private ThrottleDirection direction;
    @Indexed
    private long windowStartUtc;
    @Indexed
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

    public static String buildSampleId(String taskId, long runEpoch, long sampleSeq, ThrottlePhase phase, ThrottleDirection dir) {
        return taskId + ":" + runEpoch + ":" + sampleSeq + ":" + phase + ":" + dir;
    }
}
