package com.tapdata.tm.fieldorigin.builder;

/**
 * How a field origin was resolved.
 */
public enum MatchMethod {
    /** Exact match by the field id generated from connection, table and field name */
    ID,
    /** Matched by original field name upstream, same rule as data trace (FieldOriginalNameMapping) */
    NAME,
    /** Not matched to any source field */
    NONE
}
