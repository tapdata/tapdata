package io.tapdata.observable.alert;

/**
 * Delivers a structured task alert to TM. Implementations must not block the data thread.
 */
public interface TaskAlertPublisher {

    enum PublishResult {
        SUCCESS,
        RETRYABLE,
        NON_RETRYABLE
    }

    PublishResult publish(TaskAlertEvent event);
}
