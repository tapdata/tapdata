package com.tapdata.tm.lineage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * Lineage
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("lineage")
public class LineageEntity extends BaseEntity {
    private String name;

}