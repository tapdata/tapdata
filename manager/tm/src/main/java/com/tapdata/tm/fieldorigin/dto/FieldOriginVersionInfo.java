package com.tapdata.tm.fieldorigin.dto;

import lombok.Data;

/**
 * Current active version plus the latest build, which may have failed while the active version stays queryable.
 */
@Data
public class FieldOriginVersionInfo {
    private FieldOriginVersionDto active;
    private FieldOriginVersionDto latestBuild;
}
