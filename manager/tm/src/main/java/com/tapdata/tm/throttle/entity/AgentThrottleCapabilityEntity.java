package com.tapdata.tm.throttle.entity;

import com.tapdata.tm.commons.throttle.ThrottleDimension;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Set;

/**
 * Persisted capability registration per Agent session declaring supported meterVersion, task kinds, and paths.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document("AgentThrottleCapability")
public class AgentThrottleCapabilityEntity {

    @Id
    private String agentId;

    @Builder.Default
    private int schemaVersion = 1;
    private boolean throttleSupported;
    private String meterVersion;
    private Set<String> supportedTaskKinds;
    private Set<String> supportedPaths;
    private Set<ThrottleDimension> supportedDimensions;
    private long cdcSampleConfigVersion;
    private int cdcSampleRefreshIntervalSeconds;
    private long reportedAt;
    private long updatedAt;
}
