package com.tapdata.tm.commons.schema;

import lombok.Data;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: Zed
 * @Date: 2021/9/11
 * @Description:
 */
@Data
public class Table implements Serializable {
    private ObjectId id;
    private String tableName;
    private String metaType;
    private String type;
    private Boolean cdcEnabled;
    private String tableId;
    private List<Field> fields;
    private List<TableIndex> indices;
    private String schemaVersion;
    private Set<Integer> partitionSet;
    private FileMeta fileMeta;
    private FileProperty fileProperty;
    private String oneone;
    private String userId;
    private Map<String, Object> tableAttr;
}
