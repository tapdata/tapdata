package io.tapdata.observable.metric.throttle;

/**
 * Thrown when event-v1 metering hits nesting depth, work-unit budget, or circular-reference limits.
 */
public class MeterBudgetExceededException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MeterBudgetExceededException(String message) {
        super(message);
    }
}
