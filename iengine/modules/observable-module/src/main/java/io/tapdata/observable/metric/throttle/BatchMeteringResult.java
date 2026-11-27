package io.tapdata.observable.metric.throttle;

import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import lombok.Builder;
import lombok.Value;

import java.util.List;

/**
 * Immutable batch metering snapshot shared cleanly by token-bucket admission and 5-second telemetry aggregation.
 */
@Value
@Builder(toBuilder = true)
public class BatchMeteringResult {
    String taskId;
    long runEpoch;
    ThrottlePhase phase;
    ThrottleDirection direction;
    long dmlCount;
    long insertCount;
    long updateCount;
    long deleteCount;
    long controlCount;
    /**
     * Null signals missing or budget-limited byte estimation; 0L denotes a valid zero-byte batch.
     */
    Long estimatedBytes;
    SampleQuality sampleQuality;
    long sampleCount;
    boolean budgetHit;
    String meterVersion;
    long sampleConfigVersion;
    int cdcSampleRefreshIntervalSeconds;
    List<CdcSampleGroupKey> missingGroups;
    String sampleDiagnostics;
}
