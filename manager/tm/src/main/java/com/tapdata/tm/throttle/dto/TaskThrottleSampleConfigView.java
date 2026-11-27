package com.tapdata.tm.throttle.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * System-wide CDC sample refresh interval view including per-Agent rollout application statuses.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskThrottleSampleConfigView {

    public static final String STATUS_APPLIED = "APPLIED";
    public static final String STATUS_APPLYING = "APPLYING";
    public static final String STATUS_UNSUPPORTED_LEGACY = "UNSUPPORTED_LEGACY";

    private String key;
    private int intervalSeconds;
    private long configVersion;
    private long updatedAt;
    private List<AgentSampleStatus> agentStatuses;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AgentSampleStatus {
        private String agentId;
        private Integer appliedIntervalSeconds;
        private Long appliedConfigVersion;
        private String status;
        private String reason;
    }
}
