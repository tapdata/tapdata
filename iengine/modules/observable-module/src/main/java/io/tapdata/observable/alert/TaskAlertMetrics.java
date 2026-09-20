package io.tapdata.observable.alert;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Micrometer counters for the structured alert pipeline.
 * Labels are limited to type/code/reason to avoid high cardinality.
 */
public final class TaskAlertMetrics {
    private static final AtomicInteger QUEUE_SIZE = new AtomicInteger();

    static {
        Gauge.builder("task_alert_dispatch_queue_size", QUEUE_SIZE, AtomicInteger::get)
                .register(Metrics.globalRegistry);
    }

    private TaskAlertMetrics() {
    }

    public static void submitted(String type, String code) {
        counter("task_alert_submitted_total", type, code, null).increment();
    }

    public static void coalesced(String type, String code) {
        counter("task_alert_coalesced_total", type, code, null).increment();
    }

    public static void publishSuccess(String type, String code) {
        counter("task_alert_publish_success_total", type, code, null).increment();
    }

    public static void publishRetry(String type, String code, String reason) {
        counter("task_alert_publish_retry_total", type, code, reason).increment();
    }

    public static void publishFailed(String type, String code, String reason) {
        counter("task_alert_publish_failed_total", type, code, reason).increment();
    }

    public static void publishRejected(String type, String code) {
        counter("task_alert_publish_rejected_total", type, code, null).increment();
    }

    public static void setQueueSize(int size) {
        QUEUE_SIZE.set(size);
    }

    private static Counter counter(String name, String type, String code, String reason) {
        Counter.Builder builder = Counter.builder(name)
                .tag("type", nullToUnknown(type))
                .tag("code", nullToUnknown(code));
        if (reason != null) {
            builder.tag("reason", reason);
        }
        return builder.register(Metrics.globalRegistry);
    }

    private static String nullToUnknown(String value) {
        return value == null || value.isEmpty() ? "unknown" : value;
    }
}
