package com.tapdata.tm.fieldorigin.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Field origin of one target field in one metadata version, see TAP-13082.
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document(FieldOriginEntity.COLLECTION)
public class FieldOriginEntity extends BaseEntity {
    public static final String COLLECTION = "FieldOrigin";
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
