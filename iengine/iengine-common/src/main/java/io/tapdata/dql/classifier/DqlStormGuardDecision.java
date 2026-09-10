package io.tapdata.dql.classifier;

import io.tapdata.dql.model.DqlClassificationResult;

import java.util.Objects;

/**
 * Routing result and observability metadata produced by Storm Guard.
 */
public final class DqlStormGuardDecision {
    private final DqlClassificationResult classificationResult;
    private final DqlStormGuardKey guardKey;
    private final long windowCount;
    private final long suppressedCount;
    private final long windowStartMillis;
    private final long windowExpiresAtMillis;
    private final long maxEvents;
    private final double maxBatchRatio;
    private final double batchRatio;
    private final boolean guardTriggered;

    public DqlStormGuardDecision(DqlClassificationResult classificationResult,
                                 DqlStormGuardKey guardKey,
                                 long windowCount,
                                 long suppressedCount,
                                 long windowStartMillis,
                                 long windowExpiresAtMillis,
                                 long maxEvents,
                                 double maxBatchRatio,
                                 double batchRatio,
                                 boolean guardTriggered) {
        this.classificationResult = Objects.requireNonNull(classificationResult,
                "classificationResult must not be null");
        this.guardKey = guardKey;
        this.windowCount = windowCount;
        this.suppressedCount = suppressedCount;
        this.windowStartMillis = windowStartMillis;
        this.windowExpiresAtMillis = windowExpiresAtMillis;
        this.maxEvents = maxEvents;
        this.maxBatchRatio = maxBatchRatio;
        this.batchRatio = batchRatio;
        this.guardTriggered = guardTriggered;
    }

    public DqlClassificationResult getClassificationResult() {
        return classificationResult;
    }

    public DqlStormGuardKey getGuardKey() {
        return guardKey;
    }

    public long getWindowCount() {
        return windowCount;
    }

    public long getSuppressedCount() {
        return suppressedCount;
    }

    public long getWindowStartMillis() {
        return windowStartMillis;
    }

    public long getWindowExpiresAtMillis() {
        return windowExpiresAtMillis;
    }

    public long getMaxEvents() {
        return maxEvents;
    }

    public double getMaxBatchRatio() {
        return maxBatchRatio;
    }

    public double getBatchRatio() {
        return batchRatio;
    }

    public boolean isGuardTriggered() {
        return guardTriggered;
    }
}
