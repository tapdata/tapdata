
package com.tapdata.tm.classification.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

/**
 * 资源分类
 */
@Data
@EqualsAndHashCode(callSuper=false)
@Document("MetadataDefinition")
public class ClassificationEntity extends BaseEntity {
    /** 名称 */
    @Indexed(unique = true)
    private String value;
    /** 父id */
    private ObjectId parentId;

    @Field("item_type")
    private List<String> itemType;
}
