package com.tapdata.tm.throttle.dto;

import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Combined desired vs. last-confirmed applied policy view returned by GET/PUT /task-throttle/{kind}/{id}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskThrottlePolicyView {

    private String taskKey;
    private String tenantId;
    private String taskKind;
    private String taskId;

    private long desiredVersion;
    private Map<ThrottlePhase, ThrottlePhaseLimit> desiredPhases;
    private String updatedBy;
    private Long updatedAt;

    private long appliedVersion;
    private Map<ThrottlePhase, ThrottlePhaseLimit> appliedPhases;
    private Long appliedAt;
    private String applyStatus;
    private String lastApplyReason;

    private long currentRunEpoch;
    private String currentAgentId;
    private boolean protectedRunActive;
    private String stopGateStatus;
    private String stopGateReason;

    private TaskThrottleLiveStatusView liveStatus;
}
