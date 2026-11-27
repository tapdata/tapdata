package com.tapdata.tm.commons.throttle;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Desired task-level throttling policy contract shared between TM, Agent, and Enterprise modules.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TaskThrottlePolicyDto implements Serializable {
    private static final long serialVersionUID = 1L;

    @Builder.Default
    private int schemaVersion = 1;
    private String tenantId;
    private String taskKind;
    private String taskId;
    private long desiredVersion;
    private Map<ThrottlePhase, ThrottlePhaseLimit> phases;
    private String updatedBy;
    private Long updatedAt;
}
