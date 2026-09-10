package io.tapdata.dql.model;

/**
 * Describes how the Engine identified one business record.
 */
public enum DqlRecordIdentityType {
    PRIMARY_KEY,
    UPDATE_CONDITION,
    UNIQUE_INDEX,
    FULL_FIELD_HASH,
    UNKNOWN
}
