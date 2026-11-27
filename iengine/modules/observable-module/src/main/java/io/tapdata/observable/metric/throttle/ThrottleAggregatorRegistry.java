package io.tapdata.observable.metric.throttle;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Lightweight process-level lookup bridging task metric collectors in observable-module
 * with active {@link ThrottleMetricsWindowAggregator} instances managed by engine throttle runtimes.
 */
public final class ThrottleAggregatorRegistry {

    private static final ConcurrentHashMap<String, ThrottleMetricsWindowAggregator> AGGREGATORS =
            new ConcurrentHashMap<>();

    private ThrottleAggregatorRegistry() {
    }

    public static void register(String taskId, ThrottleMetricsWindowAggregator aggregator) {
        if (taskId == null || taskId.isBlank() || aggregator == null) {
            return;
        }
        AGGREGATORS.put(taskId.trim(), aggregator);
    }

    public static void unregister(String taskId, ThrottleMetricsWindowAggregator aggregator) {
        if (taskId == null || taskId.isBlank() || aggregator == null) {
            return;
        }
        AGGREGATORS.remove(taskId.trim(), aggregator);
    }

    public static ThrottleMetricsWindowAggregator get(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return null;
        }
        return AGGREGATORS.get(taskId.trim());
    }

    public static void clearForTest() {
        AGGREGATORS.clear();
    }
}
