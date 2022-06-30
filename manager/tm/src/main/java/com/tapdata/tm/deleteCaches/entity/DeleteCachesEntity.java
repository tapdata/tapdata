package com.tapdata.tm.deleteCaches.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;


/**
 * DeleteCaches
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("DeleteCaches")
public class DeleteCachesEntity extends BaseEntity {

    private String mongodbUri;
    private String collectionName;
    private Long timestamp;
    private Map<String, Object> data;

}