package io.tapdata.dql.classifier;

import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import io.tapdata.dql.model.DqlRouteDecision;

/**
 * Immutable settings for the unknown-error Storm Guard.
 */
public final class DqlStormGuardConfig {
    public static final long DEFAULT_WINDOW_SECONDS = 60L;
    public static final long DEFAULT_MAX_EVENTS = 20L;
    public static final double DEFAULT_MAX_BATCH_RATIO = 0.2d;
    public static final DqlRouteDecision DEFAULT_DECISION = DqlRouteDecision.TASK_RETRY;

    private final long windowSeconds;
    private final long maxEvents;
    private final double maxBatchRatio;
    private final DqlRouteDecision decision;

    public DqlStormGuardConfig(long windowSeconds,
                               long maxEvents,
                               double maxBatchRatio,
                               DqlRouteDecision decision) {
        if (windowSeconds <= 0) {
            throw new IllegalArgumentException("windowSeconds must be greater than zero");
        }
        if (windowSeconds > Long.MAX_VALUE / 1000L) {
            throw new IllegalArgumentException("windowSeconds is too large");
        }
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be greater than zero");
        }
        if (Double.isNaN(maxBatchRatio) || Double.isInfinite(maxBatchRatio)
                || maxBatchRatio < 0d || maxBatchRatio > 1d) {
            throw new IllegalArgumentException("maxBatchRatio must be between zero and one");
        }
        if (decision == null || decision == DqlRouteDecision.RECORD_DLQ) {
            throw new IllegalArgumentException("decision must be TASK_RETRY or TASK_ERROR");
        }
        this.windowSeconds = windowSeconds;
        this.maxEvents = maxEvents;
        this.maxBatchRatio = maxBatchRatio;
        this.decision = decision;
    }

    public static DqlStormGuardConfig defaults() {
        return new DqlStormGuardConfig(DEFAULT_WINDOW_SECONDS, DEFAULT_MAX_EVENTS,
                DEFAULT_MAX_BATCH_RATIO, DEFAULT_DECISION);
    }

    public static DqlStormGuardConfig from(DqlRuntimeConfig config) {
        if (config == null) {
            return defaults();
        }
        DqlRouteDecision decision;
        try {
            decision = DqlRouteDecision.valueOf(config.getUnknownGuardDecision());
        } catch (IllegalArgumentException exception) {
            decision = DEFAULT_DECISION;
        }
        return new DqlStormGuardConfig(config.getUnknownGuardWindowSeconds(),
                config.getUnknownGuardMaxEvents(), config.getUnknownGuardMaxBatchRatio(), decision);
    }

    public long getWindowSeconds() {
        return windowSeconds;
    }

    public long getWindowMillis() {
        return windowSeconds * 1000L;
    }

    public long getMaxEvents() {
        return maxEvents;
    }

    public double getMaxBatchRatio() {
        return maxBatchRatio;
    }

    public DqlRouteDecision getDecision() {
        return decision;
    }
}
