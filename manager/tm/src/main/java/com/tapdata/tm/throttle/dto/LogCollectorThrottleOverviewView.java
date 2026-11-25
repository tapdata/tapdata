package com.tapdata.tm.throttle.dto;

import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.WaitReason;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;

/**
 * Aggregated share-mining (logCollector) monitoring view separating source capture,
 * shared log storage writes, permission-filtered consumers, and redacted hidden consumer counts.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogCollectorThrottleOverviewView {

    private String collectorTaskId;
    private String collectorTaskName;
    private String collectorStatus;
    private int storageTimeDays;
    private TaskThrottlePolicyView policyView;
    private DirectionSnapshot sourceCapture;
    private DirectionSnapshot logWrite;
    private Long oldestConsumerReplayTime;
    private int totalConsumerCount;
    private int hiddenConsumerCount;
    private List<ConsumerThrottleSummary> visibleConsumers;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DirectionSnapshot {
        private Double estimatedMbPerSec;
        private Double qps;
        private WaitReason waitReason;
        private Set<ThrottleDimension> waitingDimensions;
        private LogPositionStatus logPositionStatus;
        private Long lastSampledAtUtc;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConsumerThrottleSummary {
        private String taskId;
        private String taskName;
        private String syncType;
        private String status;
        private Long consumerDelayMs;
        private Long lastEventTime;
        private boolean throttleEnabled;
        private TaskThrottleLiveStatusView liveStatus;
        private LogPositionStatus logPositionStatus;
        private Double estimatedOutputMbPerSec;
    }
}
