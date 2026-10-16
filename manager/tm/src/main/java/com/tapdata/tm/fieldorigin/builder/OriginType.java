package com.tapdata.tm.fieldorigin.builder;

/**
 * Field origin classification, see TAP-13082 §10.1.
 */
public enum OriginType {
    /** Target field comes directly from a source field with the same name */
    DIRECT,
    /** Target field comes from a source field that was renamed on the way */
    RENAMED,
    /** Target field is computed by a processor node */
    DERIVED,
    /** Map/Array container created by a merge node, not a physical field */
    CONTAINER,
    /** Field created manually by a field add node without upstream source */
    MANUAL,
    /** Origin can not be resolved reliably */
    UNKNOWN
}
