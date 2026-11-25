package com.tapdata.tm.throttle.dto;

import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * CAS policy update payload accepting ONLY throttling policy fields and expectedVersion.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TaskThrottlePolicyUpdateRequest {
    private long expectedVersion;
    private Map<ThrottlePhase, ThrottlePhaseLimit> phases;
}
