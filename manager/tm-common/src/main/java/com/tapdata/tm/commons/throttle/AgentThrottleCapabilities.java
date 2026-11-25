package com.tapdata.tm.commons.throttle;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

/**
 * Agent throttling and metering capability declaration reported at startup/heartbeat registration.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AgentThrottleCapabilities implements Serializable {
    private static final long serialVersionUID = 1L;

    @Builder.Default
    private int schemaVersion = 1;
    private String agentId;
    private boolean throttleSupported;
    @Builder.Default
    private String meterVersion = ThrottleConstants.METER_VERSION;
    private Set<String> supportedTaskKinds;
    private Set<String> supportedPaths;
    private Set<ThrottleDimension> supportedDimensions;
    private long cdcSampleConfigVersion;
    private int cdcSampleRefreshIntervalSeconds;
    private long reportedAt;
}
