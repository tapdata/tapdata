package com.tapdata.tm.metadataInstancesCompare.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.schema.DifferenceField;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("MetadataInstancesCompare")
public class MetadataInstancesCompareEntity extends BaseEntity {
    private String tableName;
    private String qualifiedName;
    private String nodeId;
    private List<DifferenceField> differenceFieldList;
    private String type;
    private String status;
    private Date targetSchemaLoadTime;
    private String taskId;
}
