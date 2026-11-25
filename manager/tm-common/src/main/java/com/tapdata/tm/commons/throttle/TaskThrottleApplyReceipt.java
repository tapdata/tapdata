package com.tapdata.tm.commons.throttle;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Agent apply receipt reported back to TM after validating and atomically replacing task policy snapshot.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TaskThrottleApplyReceipt implements Serializable {
    private static final long serialVersionUID = 1L;

    private String taskKey;
    private String agentId;
    private long runEpoch;
    private long policyVersion;
    private long appliedAt;
    private String outcome;
    private String reason;
    private List<Long> skippedVersions;
}
