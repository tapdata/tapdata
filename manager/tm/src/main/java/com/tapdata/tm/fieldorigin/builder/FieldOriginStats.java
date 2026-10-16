package com.tapdata.tm.fieldorigin.builder;

import lombok.Data;

/**
 * Build coverage, see TAP-13082 §11.3.
 */
@Data
public class FieldOriginStats {
    private int targetTables;
    private int totalFields;
    private int resolvedFields;
    private int unknownFields;
    private int multiSourceFields;
    private int containerFields;
    private int derivedFields;
    private int manualFields;
    private int nameMatchedFields;
}
