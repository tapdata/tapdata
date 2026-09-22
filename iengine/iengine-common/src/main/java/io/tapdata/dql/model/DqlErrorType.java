package io.tapdata.dql.model;

/**
 * Record-level error categories persisted by TM.
 */
public enum DqlErrorType {
    MALFORMED_RECORD,
    POISON_RECORD,
    TRANSFORM_ERROR,
    TARGET_WRITE_ERROR,
    UNKNOWN_RECORD_ERROR
}
