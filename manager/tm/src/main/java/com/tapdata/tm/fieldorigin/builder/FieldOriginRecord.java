package com.tapdata.tm.fieldorigin.builder;

import lombok.Data;

/**
 * One origin of one target field. A target field may have several records (multi source, duplicate tables).
 */
@Data
public class FieldOriginRecord {
    private String targetNodeId;
    private String targetConnectionId;
    private String targetTable;
    private String fieldPath;
    private String normalizedFieldPath;
    private String sourceNodeId;
    private String sourceConnectionId;
    private String sourceTable;
    private String sourceField;
    private OriginType originType;
    private MatchMethod matchMethod;
    private String transformNodeId;
    private String mergeNodeId;
    private String mergePropertyId;
    private String targetPath;

    public FieldOriginRecord copy() {
        FieldOriginRecord copy = new FieldOriginRecord();
        copy.setTargetNodeId(targetNodeId);
        copy.setTargetConnectionId(targetConnectionId);
        copy.setTargetTable(targetTable);
        copy.setFieldPath(fieldPath);
        copy.setNormalizedFieldPath(normalizedFieldPath);
        copy.setSourceNodeId(sourceNodeId);
        copy.setSourceConnectionId(sourceConnectionId);
        copy.setSourceTable(sourceTable);
        copy.setSourceField(sourceField);
        copy.setOriginType(originType);
        copy.setMatchMethod(matchMethod);
        copy.setTransformNodeId(transformNodeId);
        copy.setMergeNodeId(mergeNodeId);
        copy.setMergePropertyId(mergePropertyId);
        copy.setTargetPath(targetPath);
        return copy;
    }

    /** Identity of the record inside one version, same columns as the unique index. */
    public String identity() {
        return String.join("\u0001", String.valueOf(targetNodeId), String.valueOf(targetTable), String.valueOf(fieldPath),
                String.valueOf(sourceNodeId), String.valueOf(sourceTable), String.valueOf(sourceField),
                String.valueOf(mergePropertyId), String.valueOf(targetPath), String.valueOf(originType),
                String.valueOf(transformNodeId));
    }
}
