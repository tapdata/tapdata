package com.tapdata.tm.datacategory.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;


/**
 * DataCategory
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("DataCategory")
public class DataCategoryEntity extends BaseEntity {
    private String name;
    @Field("connection_id")
    private ObjectId connectionId;
}