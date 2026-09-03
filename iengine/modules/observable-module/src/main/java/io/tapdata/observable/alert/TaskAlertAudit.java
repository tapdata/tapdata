package io.tapdata.observable.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Root-logger audit for the structured alert pipeline.
 * Must never write back into {@code ObsLogger} to avoid recursion.
 */
public final class TaskAlertAudit {
    public static final String TASK_ALERT_INVALID = "TASK_ALERT_INVALID";
    public static final String TASK_ALERT_QUEUE_REJECTED = "TASK_ALERT_QUEUE_REJECTED";
    public static final String TASK_ALERT_PUBLISH_RETRY = "TASK_ALERT_PUBLISH_RETRY";
    public static final String TASK_ALERT_PUBLISH_FAILED = "TASK_ALERT_PUBLISH_FAILED";
    public static final String TASK_ALERT_TM_REJECTED = "TASK_ALERT_TM_REJECTED";
    public static final String TASK_ALERT_TEMPLATE_FAILED = "TASK_ALERT_TEMPLATE_FAILED";

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskAlertAudit.class);

    private TaskAlertAudit() {
    }

    public static void invalid(String reason, Throwable throwable) {
        LOGGER.error("{} reason={}", TASK_ALERT_INVALID, reason, throwable);
    }

    public static void queueRejected(String type, String code, long rejectedCount) {
        LOGGER.error("{} type={} code={} rejectedCount={}", TASK_ALERT_QUEUE_REJECTED, type, code, rejectedCount);
    }

    public static void publishRetry(String type, String code, String reason, int attempt) {
        LOGGER.warn("{} type={} code={} reason={} attempt={}", TASK_ALERT_PUBLISH_RETRY, type, code, reason, attempt);
    }

    public static void publishFailed(String type, String code, String reason, Throwable throwable) {
        LOGGER.error("{} type={} code={} reason={}", TASK_ALERT_PUBLISH_FAILED, type, code, reason, throwable);
    }

    public static void tmRejected(String type, String code, String reason) {
        LOGGER.error("{} type={} code={} reason={}", TASK_ALERT_TM_REJECTED, type, code, reason);
    }
}
