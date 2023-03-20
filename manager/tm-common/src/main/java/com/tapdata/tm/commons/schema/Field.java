
package com.tapdata.tm.commons.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Field implements Serializable {

    public static final String SOURCE_JOB_ANALYZE = "job_analyze";
    public static final String SOURCE_AUTO = "auto";
    public static final String SOURCE_MANUAL = "manual";

    private Object autoincrement;

    private Integer columnSize;
    @JsonProperty("data_code")
    @org.springframework.data.mongodb.core.mapping.Field("data_code")
    private Integer dataCode;
    @JsonProperty("dataType")
    @org.springframework.data.mongodb.core.mapping.Field("dataType")
    private Integer dataType1;
    @JsonProperty("tapType")
    private String tapType;
    @JsonProperty("data_type")
    @org.springframework.data.mongodb.core.mapping.Field("data_type")
    private String dataType;
    private String selectDataType;
    private String dataTypeTemp;

    @JsonProperty("default_value")
    @org.springframework.data.mongodb.core.mapping.Field("default_value")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Object defaultValue;

    private Object originalDefaultValue;

    @JsonProperty("field_name")
    @org.springframework.data.mongodb.core.mapping.Field("field_name")
    private String fieldName;
    @JsonProperty("foreign_key_position")
    @org.springframework.data.mongodb.core.mapping.Field("foreign_key_position")
    private Integer foreignKeyPosition;

    @org.springframework.data.mongodb.core.mapping.Field("id")
    private String id;

    public void setId(String id) {
        this.id = id;
    }

    private Boolean isAnalyze;
    @JsonProperty("is_auto_allowed")
    @org.springframework.data.mongodb.core.mapping.Field("is_auto_allowed")
    private Boolean isAutoAllowed;
    @JsonProperty("is_deleted")
    @org.springframework.data.mongodb.core.mapping.Field("is_deleted")
    private boolean isDeleted = false;
    @JsonProperty("is_nullable")
    @org.springframework.data.mongodb.core.mapping.Field("is_nullable")
    private Object isNullable;

    private Boolean isPrecisionEdit;

    private Boolean isScaleEdit;
    @JsonProperty("java_type")
    @org.springframework.data.mongodb.core.mapping.Field("java_type")
    private String javaType;
    @JsonProperty("javaType")
    @org.springframework.data.mongodb.core.mapping.Field("javaType")
    private String javaType1;

    private Integer oriPrecision;

    private Object oriScale;
    @JsonProperty("original_field_name")
    @org.springframework.data.mongodb.core.mapping.Field("original_field_name")
    private String originalFieldName;
    @JsonProperty("original_java_type")
    @org.springframework.data.mongodb.core.mapping.Field("original_java_type")
    private String originalJavaType;

    private Object parent;

    private Integer precision;
    private Integer originalPrecision;

    @JsonProperty("primaryKey")
    @org.springframework.data.mongodb.core.mapping.Field("primaryKey")
    private Boolean primaryKey;
    @JsonProperty("primary_key_position")
    @org.springframework.data.mongodb.core.mapping.Field("primary_key_position")
    private Integer primaryKeyPosition;

    private Integer scale;
    private Integer originalScale;

    private String source;
    private boolean unique = false;
    private String key;
    @JsonProperty("pk_constraint_name")
    @org.springframework.data.mongodb.core.mapping.Field("pk_constraint_name")
    private String pkConstraintName;
    @JsonProperty("pkConstraintName")
    @org.springframework.data.mongodb.core.mapping.Field("pkConstraintName")
    private String pkConstraintName1;
    @JsonProperty("foreign_key")
    @org.springframework.data.mongodb.core.mapping.Field("foreign_key")
    private Boolean foreignKey;
    @JsonProperty("foreign_key_table")
    @org.springframework.data.mongodb.core.mapping.Field("foreign_key_table")
    private String foreignKeyTable;
    @JsonProperty("foreign_key_column")
    @org.springframework.data.mongodb.core.mapping.Field("foreign_key_column")
    private String foreignKeyColumn;

    @JsonProperty("node_data_type")
    @org.springframework.data.mongodb.core.mapping.Field("node_data_type")
    private String nodeDataType;

    @JsonProperty("table_name")
    @org.springframework.data.mongodb.core.mapping.Field("table_name")
    private String tableName;
    @JsonProperty("alias_name")
    @org.springframework.data.mongodb.core.mapping.Field("alias_name")
    private String aliasName;

    private Boolean visible;

    private String comment;
    private Integer columnPosition;
    /**
     * 模型推演时，用来记录字段在源库中的数据类型
     */
    private String originalDataType;
    private String field_type;
    private String required;
    private String example;
    private List<DictionaryDto> dictionary;
    private List<String> oldIdList;

    private String sourceDbType;

    /**
     * 临时字段，在处理字段类型映射时，用于声明这个字段在源库的 typeMapping 规则是否 fixed
     */
    private Boolean fixed;

    private Boolean dataTypeSupport;
    private String createSource; // manual/auto/job_analyze
    private String changeRuleId; // 如果命中字段变更规则则存在值

    private boolean useDefaultValue = true;

    public boolean getUseDefaultValue() {
        return useDefaultValue;
    }

    public void setUseDefaultValue(boolean useDefaultValue) {
        this.useDefaultValue = useDefaultValue;
    }
}
