
package com.tapdata.tm.metadatainstance.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class FieldVo extends BaseVo {
    private Object autoincrement;

    private Integer columnSize;
    @JsonProperty("data_code")
    private Integer dataCode;
    @JsonProperty("data_type")
    private String dataType;
    @JsonProperty("dataType")
    private Integer dataType1;
    @JsonProperty("default_value")
    private Object defaultValue;
    @JsonProperty("field_name")
    private String fieldName;
    @JsonProperty("foreign_key_position")
    private Integer foreignKeyPosition;

    private String id;

    private Boolean isAnalyze;
    @JsonProperty("is_auto_allowed")
    private Boolean isAutoAllowed;
    @JsonProperty("is_deleted")
    private boolean isDeleted = false;
    @JsonProperty("is_nullable")
    private Object isNullable;

    private Boolean isPrecisionEdit;

    private Boolean isScaleEdit;
    @JsonProperty("java_type")
    private String javaType;
    @JsonProperty("javaType")
    private String javaType1;

    private Object oriPrecision;

    private Object oriScale;
    @JsonProperty("original_field_name")
    private String originalFieldName;
    @JsonProperty("original_java_type")
    private String originalJavaType;

    private Object parent;

    private Integer precision;
    @JsonProperty("primaryKey")
    private Boolean primaryKey;
    @JsonProperty("primary_key_position")
    private Integer primaryKeyPosition;

    private Integer scale;

    private String source;
    private boolean unique = false;
    private String key;
    @JsonProperty("pk_constraint_name")
    private String pkConstraintName;
    @JsonProperty("pkConstraintName")
    private String pkConstraintName1;
    @JsonProperty("foreign_key")
    private Boolean foreignKey;
    @JsonProperty("foreign_key_table")
    private String foreignKeyTable;
    @JsonProperty("foreign_key_column")
    private String foreignKeyColumn;

    @JsonProperty("node_data_type")
    private String nodeDataType;

    @JsonProperty("table_name")
    private String tableName;
    @JsonProperty("alias_name")
    private String aliasName;

    private Boolean visible;

    private String comment;
    private Integer columnPosition;
    /**
     * 模型推演时，用来记录字段在源库中的数据类型
     */
    private String originalDataType;

}
