package com.tapdata.tm.commons.throttle;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Incremental policy, run-epoch authorization, and CDC sample config delta response pulled by an Agent.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AgentThrottleDeltaResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    @Builder.Default
    private int schemaVersion = 1;
    private long cursor;
    private boolean fullSync;
    private long serverTimeUtc;
    private long sampleConfigVersion;
    private int cdcSampleRefreshIntervalSeconds;
    private List<TaskDirective> directives;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TaskDirective implements Serializable {
        private static final long serialVersionUID = 1L;

        private String tenantId;
        private String taskKind;
        private String taskId;
        private long authorizedRunEpoch;
        private boolean protectedRun;
        private boolean fenced;
        private TaskThrottlePolicyDto policy;
    }
}
