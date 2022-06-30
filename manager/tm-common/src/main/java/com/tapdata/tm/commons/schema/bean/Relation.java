
package com.tapdata.tm.commons.schema.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class Relation {

    @JsonProperty("fields")
    private List<RelationField> relationFields;
    private String id;
    @JsonProperty("table_name")
    private String tableName;
    @JsonProperty("foreign_key_table")
    private ForeignKeyTable foreignKeyTable;
    @JsonProperty("foreign_key_column")
    private String foreignKeyColumn;

    /**
     * onemany  一对多
     */
    private String rel;


}
