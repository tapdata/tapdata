package com.tapdata.tm.DataCatalogs.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * DataCatalogs
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("DataCatalog")
public class DataCatalogsEntity extends BaseEntity {
    private String name;
    private String tags;
    private List<Map<String,String>> conn_info;
    private String asset_desc;
    private String collection;
    private ObjectId connection_id;
    private Date create_time;
    private String database;
    private Date lastModified;
    private Integer total_docs;
    private Integer violated_docs;
    private Integer violated_percentage;


}
