package com.tapdata.tm.commons.schema.bean;

import com.tapdata.tm.commons.schema.DictionaryDto;
import io.tapdata.entity.schema.TapField;
import lombok.ToString;

import java.util.List;

@ToString
public class TapFieldEx extends TapField {
    private String id;

    private String aliasName;

    private Boolean visible;
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
    private String nodeDataType;

    private Integer oriPrecision;

    private Object oriScale;
    private String originalFieldName;
    private String originalJavaType;

    private Object parent;

    private Integer precision;

    private Boolean isPrecisionEdit;

    private Boolean isScaleEdit;
    private String javaType;
    private String javaType1;
    private Integer dataCode;
    private Integer dataType1;

    private String createSource;
    private String source;


    private Object autoincrement;

    private Integer columnSize;
    private String dataTypeTemp;

    private Object originalDefaultValue;

    private String fieldName;
    private Integer foreignKeyPosition;

    private Boolean isAnalyze;
    private Boolean isAutoAllowed;
    private boolean isDeleted = false;
    private Object isNullable;


    private Integer originalPrecision;
    private Boolean primaryKey;
    private Integer primaryKeyPosition;

    private Integer scale;
    private Integer originalScale;

    private boolean unique = false;
    private String key;
    private String pkConstraintName;
    private String pkConstraintName1;
    private Boolean foreignKey;
    private String foreignKeyColumn;

    private String tableName;
    private Integer columnPosition;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    public Boolean getVisible() {
        return visible;
    }

    public void setVisible(Boolean visible) {
        this.visible = visible;
    }

    public String getOriginalDataType() {
        return originalDataType;
    }

    public void setOriginalDataType(String originalDataType) {
        this.originalDataType = originalDataType;
    }

    public String getField_type() {
        return field_type;
    }

    public void setField_type(String field_type) {
        this.field_type = field_type;
    }

    public String getRequired() {
        return required;
    }

    public void setRequired(String required) {
        this.required = required;
    }

    public String getExample() {
        return example;
    }

    public void setExample(String example) {
        this.example = example;
    }

    public List<DictionaryDto> getDictionary() {
        return dictionary;
    }

    public void setDictionary(List<DictionaryDto> dictionary) {
        this.dictionary = dictionary;
    }

    public List<String> getOldIdList() {
        return oldIdList;
    }

    public void setOldIdList(List<String> oldIdList) {
        this.oldIdList = oldIdList;
    }

    public String getNodeDataType() {
        return nodeDataType;
    }

    public void setNodeDataType(String nodeDataType) {
        this.nodeDataType = nodeDataType;
    }

    public Integer getOriPrecision() {
        return oriPrecision;
    }

    public void setOriPrecision(Integer oriPrecision) {
        this.oriPrecision = oriPrecision;
    }

    public Object getOriScale() {
        return oriScale;
    }

    public void setOriScale(Object oriScale) {
        this.oriScale = oriScale;
    }

    public String getOriginalFieldName() {
        return originalFieldName;
    }

    public void setOriginalFieldName(String originalFieldName) {
        this.originalFieldName = originalFieldName;
    }

    public String getOriginalJavaType() {
        return originalJavaType;
    }

    public void setOriginalJavaType(String originalJavaType) {
        this.originalJavaType = originalJavaType;
    }

    public Object getParent() {
        return parent;
    }

    public void setParent(Object parent) {
        this.parent = parent;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Boolean getPrecisionEdit() {
        return isPrecisionEdit;
    }

    public void setPrecisionEdit(Boolean precisionEdit) {
        isPrecisionEdit = precisionEdit;
    }

    public Boolean getScaleEdit() {
        return isScaleEdit;
    }

    public void setScaleEdit(Boolean scaleEdit) {
        isScaleEdit = scaleEdit;
    }

    public String getJavaType() {
        return javaType;
    }

    public void setJavaType(String javaType) {
        this.javaType = javaType;
    }

    public String getJavaType1() {
        return javaType1;
    }

    public void setJavaType1(String javaType1) {
        this.javaType1 = javaType1;
    }

    public Integer getDataCode() {
        return dataCode;
    }

    public void setDataCode(Integer dataCode) {
        this.dataCode = dataCode;
    }

    public Integer getDataType1() {
        return dataType1;
    }

    public void setDataType1(Integer dataType1) {
        this.dataType1 = dataType1;
    }

    public String getCreateSource() {
        return createSource;
    }

    public void setCreateSource(String createSource) {
        this.createSource = createSource;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSourceDbType() {
        return sourceDbType;
    }

    public void setSourceDbType(String sourceDbType) {
        this.sourceDbType = sourceDbType;
    }

    public Object getAutoincrement() {
        return autoincrement;
    }

    public void setAutoincrement(Object autoincrement) {
        this.autoincrement = autoincrement;
    }

    public Integer getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(Integer columnSize) {
        this.columnSize = columnSize;
    }

    public String getDataTypeTemp() {
        return dataTypeTemp;
    }

    public void setDataTypeTemp(String dataTypeTemp) {
        this.dataTypeTemp = dataTypeTemp;
    }

    public Object getOriginalDefaultValue() {
        return originalDefaultValue;
    }

    public void setOriginalDefaultValue(Object originalDefaultValue) {
        this.originalDefaultValue = originalDefaultValue;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Integer getForeignKeyPosition() {
        return foreignKeyPosition;
    }

    public void setForeignKeyPosition(Integer foreignKeyPosition) {
        this.foreignKeyPosition = foreignKeyPosition;
    }

    public Boolean getAnalyze() {
        return isAnalyze;
    }

    public void setAnalyze(Boolean analyze) {
        isAnalyze = analyze;
    }

    public Boolean getAutoAllowed() {
        return isAutoAllowed;
    }

    public void setAutoAllowed(Boolean autoAllowed) {
        isAutoAllowed = autoAllowed;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }

    public Object getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(Object isNullable) {
        this.isNullable = isNullable;
    }

    public Integer getOriginalPrecision() {
        return originalPrecision;
    }

    public void setOriginalPrecision(Integer originalPrecision) {
        this.originalPrecision = originalPrecision;
    }

    @Override
    public Boolean getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public void setPrimaryKey(Boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Integer getPrimaryKeyPosition() {
        return primaryKeyPosition;
    }

    public void setPrimaryKeyPosition(Integer primaryKeyPosition) {
        this.primaryKeyPosition = primaryKeyPosition;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Integer getOriginalScale() {
        return originalScale;
    }

    public void setOriginalScale(Integer originalScale) {
        this.originalScale = originalScale;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getPkConstraintName() {
        return pkConstraintName;
    }

    public void setPkConstraintName(String pkConstraintName) {
        this.pkConstraintName = pkConstraintName;
    }

    public String getPkConstraintName1() {
        return pkConstraintName1;
    }

    public void setPkConstraintName1(String pkConstraintName1) {
        this.pkConstraintName1 = pkConstraintName1;
    }

    public Boolean getForeignKey() {
        return foreignKey;
    }

    public void setForeignKey(Boolean foreignKey) {
        this.foreignKey = foreignKey;
    }

    public String getForeignKeyColumn() {
        return foreignKeyColumn;
    }

    public void setForeignKeyColumn(String foreignKeyColumn) {
        this.foreignKeyColumn = foreignKeyColumn;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getColumnPosition() {
        return columnPosition;
    }

    public void setColumnPosition(Integer columnPosition) {
        this.columnPosition = columnPosition;
    }
}
