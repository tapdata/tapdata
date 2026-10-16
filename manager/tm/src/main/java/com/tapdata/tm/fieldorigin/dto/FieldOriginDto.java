package com.tapdata.tm.fieldorigin.dto;

import lombok.Data;

@Data
public class FieldOriginDto {
    private String taskId;
    private Long version;
    private String targetNodeId;
    private String targetConnectionId;
    private String targetTable;
    private String fieldPath;
    private String normalizedFieldPath;
    private String sourceNodeId;
    private String sourceConnectionId;
    private String sourceTable;
    private String sourceField;
    private String originType;
    private String matchMethod;
    private String transformNodeId;
    private String mergeNodeId;
    private String mergePropertyId;
    private String targetPath;
}
