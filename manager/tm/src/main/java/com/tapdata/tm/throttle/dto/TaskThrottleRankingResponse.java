package com.tapdata.tm.throttle.dto;

import com.tapdata.tm.commons.throttle.ThrottleDirection;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Dashboard estimated data rate ranking (MB/s) using union-deduplicated coverage >= 90%.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskThrottleRankingResponse {

    private String meterVersion;
    private ThrottleDirection direction;
    private long windowSeconds;
    private double minCoverageRatio;
    private List<RankingItem> items;
    private List<ExcludedItem> excluded;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RankingItem {
        private String taskId;
        private String taskName;
        private String taskKind;
        private double estimatedBytesPerSec;
        private double estimatedMbPerSec;
        private long totalEstimatedBytes;
        private double coveredSeconds;
        private double coverageRatio;
        private boolean throttleEnabled;
        private TaskThrottleLiveStatusView liveStatus;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ExcludedItem {
        private String taskId;
        private String taskName;
        private String taskKind;
        private double coveredSeconds;
        private double coverageRatio;
        private String reason;
    }
}
