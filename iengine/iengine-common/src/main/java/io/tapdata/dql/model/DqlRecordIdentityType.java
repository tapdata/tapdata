package io.tapdata.dql.model;

/**
 * Describes how the Engine identified one business record.
 */
public enum DqlRecordIdentityType {
    PRIMARY_KEY,
    UNIQUE_INDEX,
    FULL_FIELD_HASH,
    UNKNOWN
}
