package io.tapdata.dql.model;

/**
 * Scope assigned by the Engine classifier before an error is routed.
 */
public enum DqlExceptionScope {
    RECORD,
    TASK_SHARED,
    SYSTEM,
    UNKNOWN
}
