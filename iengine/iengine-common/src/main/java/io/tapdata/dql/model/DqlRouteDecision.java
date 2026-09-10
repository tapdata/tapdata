package io.tapdata.dql.model;

/**
 * Engine routing decision for an exception.
 */
public enum DqlRouteDecision {
    RECORD_DLQ,
    TASK_RETRY,
    TASK_ERROR
}
