package com.tapdata.tm.commons.schema.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.FileProperty;
import com.tapdata.tm.commons.schema.TableIndex;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: Zed
 * @Date: 2021/9/11
 * @Description:
 */
@Data
public class Table {
    private ObjectId id;
    @JsonProperty("table_name")
    @Field("table_name")
    private String tableName;
    @JsonProperty("meta_type")
    @Field("meta_type")
    private String metaType;
    private String type;
    @JsonProperty("cdc_enabled")
    @Field("cdc_enabled")
    private Boolean cdcEnabled;
    private String tableId;
    private List<com.tapdata.tm.commons.schema.Field> fields;
    private List<Map<String, Object>> indexes;
    private List<TableIndex> indices;
    private String schemaVersion;
    /** kafka队列的分区列表 */
    private Set<Integer> partitionSet;
    private FileMeta fileMeta;
    private FileProperty fileProperty;
    private String oneone;
    private String userId;
}
