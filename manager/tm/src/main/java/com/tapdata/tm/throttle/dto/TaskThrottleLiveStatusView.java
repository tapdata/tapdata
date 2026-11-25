package com.tapdata.tm.throttle.dto;

import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.WaitReason;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Evaluated real-time throttling status for task list tooltips and monitoring banners.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskThrottleLiveStatusView {

    private String taskId;
    private boolean throttlingActive;
    private boolean statusUnknown;
    private boolean policyConfigured;
    private Set<ThrottlePhase> configuredPhases;
    private long runEpoch;
    private Long lastReportedAtUtc;
    private List<ActiveWaitDetail> activeWaits;
    private Map<ThrottlePhase, Set<ThrottleDimension>> activeDimensionsByPhase;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ActiveWaitDetail {
        private ThrottlePhase phase;
        private ThrottleDirection direction;
        private WaitReason waitReason;
        private Set<ThrottleDimension> waitingDimensions;
        private long appliedVersion;
        private double bufferWatermark;
        private LogPositionStatus logPositionStatus;
    }
}
