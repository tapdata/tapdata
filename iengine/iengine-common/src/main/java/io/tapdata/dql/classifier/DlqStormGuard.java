package io.tapdata.dql.classifier;

import io.tapdata.dql.model.DqlClassificationConfidence;
import io.tapdata.dql.model.DqlClassificationResult;
import io.tapdata.dql.model.DqlErrorType;
import io.tapdata.dql.model.DqlExceptionScope;
import io.tapdata.dql.model.DqlRouteDecision;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * Thread-safe in-memory protection for unknown single-record failures.
 */
public final class DlqStormGuard {
    private final DqlStormGuardConfig config;
    private final LongSupplier timeMillis;
    private final ConcurrentMap<DqlStormGuardKey, WindowState> windows = new ConcurrentHashMap<>();

    public DlqStormGuard() {
        this(DqlStormGuardConfig.defaults(), System::currentTimeMillis);
    }

    public DlqStormGuard(DqlStormGuardConfig config) {
        this(config, System::currentTimeMillis);
    }

    public DlqStormGuard(DqlStormGuardConfig config, LongSupplier timeMillis) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (timeMillis == null) {
            throw new IllegalArgumentException("timeMillis must not be null");
        }
        this.config = config;
        this.timeMillis = timeMillis;
    }

    public DqlStormGuardDecision evaluate(DqlClassificationResult classification,
                                          DqlStormGuardContext context) {
        if (classification == null) {
            throw new IllegalArgumentException("classification must not be null");
        }
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }

        if (classification.getExceptionScope() != DqlExceptionScope.UNKNOWN) {
            return new DqlStormGuardDecision(copy(classification), null, 0L, 0L,
                    -1L, -1L, config.getMaxEvents(), config.getMaxBatchRatio(),
                    context.getBatchRatio(), false);
        }

        DqlStormGuardKey guardKey = context.getGuardKey();
        long now = now();
        double batchRatio = context.getBatchRatio();
        if (!context.isSingleEventLocatable()) {
            DqlClassificationResult taskResult = taskResult(classification,
                    "Storm Guard rejected unknown failure because the single event is not locatable");
            return new DqlStormGuardDecision(taskResult, guardKey, 0L, 1L, now,
                    expiration(now), config.getMaxEvents(), config.getMaxBatchRatio(),
                    batchRatio, true);
        }

        boolean batchRatioExceeded = batchRatio >= 0d && batchRatio > config.getMaxBatchRatio();
        WindowState state = windows.compute(guardKey, (key, previous) -> {
            WindowState current = previous;
            if (current == null || now >= current.expiresAtMillis) {
                current = new WindowState(now, expiration(now), 0L, false, 0L);
            }
            long count = increment(current.count);
            boolean triggered = current.triggered
                    || count > config.getMaxEvents()
                    || batchRatioExceeded;
            long suppressed = triggered ? increment(current.suppressedCount) : current.suppressedCount;
            return new WindowState(current.windowStartMillis, current.expiresAtMillis,
                    count, triggered, suppressed);
        });

        DqlClassificationResult result = state.triggered
                ? taskResult(classification, triggerReason(state, batchRatio))
                : allowedResult(classification, state, batchRatio);
        return new DqlStormGuardDecision(result, guardKey, state.count, state.suppressedCount,
                state.windowStartMillis, state.expiresAtMillis, config.getMaxEvents(),
                config.getMaxBatchRatio(), batchRatio, state.triggered);
    }

    public DqlClassificationResult protect(DqlClassificationResult classification,
                                            DqlStormGuardContext context) {
        return evaluate(classification, context).getClassificationResult();
    }

    /**
     * Removes expired windows so a long-running task does not retain one entry per historical key.
     */
    public int clearExpired() {
        long now = now();
        AtomicInteger removed = new AtomicInteger();
        for (Map.Entry<DqlStormGuardKey, WindowState> entry : windows.entrySet()) {
            if (now >= entry.getValue().expiresAtMillis
                    && windows.remove(entry.getKey(), entry.getValue())) {
                removed.incrementAndGet();
            }
        }
        return removed.get();
    }

    public int getActiveWindowCount() {
        return windows.size();
    }

    private DqlClassificationResult allowedResult(DqlClassificationResult source,
                                                  WindowState state,
                                                  double batchRatio) {
        DqlClassificationResult result = copy(source);
        result.setExceptionScope(DqlExceptionScope.UNKNOWN);
        result.setRouteDecision(DqlRouteDecision.RECORD_DLQ);
        result.setErrorType(DqlErrorType.UNKNOWN_RECORD_ERROR);
        result.setClassificationConfidence(DqlClassificationConfidence.UNKNOWN_SINGLE);
        result.setClassificationReason("Storm Guard allowed unknown single record: count="
                + state.count + "/" + config.getMaxEvents()
                + ", batchRatio=" + ratioText(batchRatio));
        return result;
    }

    private DqlClassificationResult taskResult(DqlClassificationResult source, String reason) {
        DqlClassificationResult result = copy(source);
        result.setExceptionScope(DqlExceptionScope.UNKNOWN);
        result.setRouteDecision(config.getDecision());
        result.setErrorType(null);
        result.setClassificationConfidence(DqlClassificationConfidence.RULE);
        result.setClassificationReason(reason);
        return result;
    }

    private String triggerReason(WindowState state, double batchRatio) {
        String countReason = "count=" + state.count + "/" + config.getMaxEvents();
        String ratioReason = batchRatio >= 0d
                ? ", batchRatio=" + ratioText(batchRatio) + "/" + config.getMaxBatchRatio()
                : ", batchRatio=not_applicable";
        return "Storm Guard triggered: " + countReason + ratioReason
                + ", decision=" + config.getDecision()
                + ", suppressed=" + state.suppressedCount;
    }

    private String ratioText(double ratio) {
        return ratio < 0d ? "not_applicable" : String.valueOf(ratio);
    }

    private long now() {
        return timeMillis.getAsLong();
    }

    private long expiration(long start) {
        long windowMillis = config.getWindowMillis();
        return start > Long.MAX_VALUE - windowMillis
                ? Long.MAX_VALUE : start + windowMillis;
    }

    private long increment(long value) {
        return value == Long.MAX_VALUE ? Long.MAX_VALUE : value + 1L;
    }

    private DqlClassificationResult copy(DqlClassificationResult source) {
        DqlClassificationResult result = new DqlClassificationResult();
        result.setExceptionScope(source.getExceptionScope());
        result.setRouteDecision(source.getRouteDecision());
        result.setErrorType(source.getErrorType());
        result.setClassificationReason(source.getClassificationReason());
        result.setClassificationConfidence(source.getClassificationConfidence());
        return result;
    }

    private static final class WindowState {
        private final long windowStartMillis;
        private final long expiresAtMillis;
        private final long count;
        private final boolean triggered;
        private final long suppressedCount;

        private WindowState(long windowStartMillis,
                            long expiresAtMillis,
                            long count,
                            boolean triggered,
                            long suppressedCount) {
            this.windowStartMillis = windowStartMillis;
            this.expiresAtMillis = expiresAtMillis;
            this.count = count;
            this.triggered = triggered;
            this.suppressedCount = suppressedCount;
        }
    }
}
