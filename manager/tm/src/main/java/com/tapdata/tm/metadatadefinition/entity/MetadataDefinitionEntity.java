package com.tapdata.tm.metadatadefinition.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;


/**
 * MetadataDefinition
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("MetadataDefinition")
public class MetadataDefinitionEntity extends BaseEntity {
    private String value;

    @Field("parent_id")
    private String parent_id;

    @Field("item_type")
    private List<String> itemType;
    private String desc;
    private String linkId;
    private Boolean readOnly;
}